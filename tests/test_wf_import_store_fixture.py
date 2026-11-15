# ruff: noqa: S608  # Why: every test SQL identifier is the fixture-owned random scratch schema or a module-constant table name; every data value is $N-bound.
"""Direct real-PostgreSQL behavior controls for the generic import domain store.

This is the *fixture-local* domain lane, labelled explicitly: it runs no TaskQ
workflow, no worker, no engine migration, no fleet, no sample application and
no external service. It proves only ``examples/adoption/import_store.py`` at its
caller-provided-connection boundary.

Every type/status test below is a direct database observation, not a mocked
result. Two independent pools/connections are used where visibility or
lost-acknowledgement behavior matters. ``pg_terminate_backend`` is real. All
scratch schemas are dropped by the fixture; no pool or connection is retained by
the store.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
from collections.abc import Callable, Coroutine, Iterator, Sequence
from decimal import Decimal
from functools import wraps
from types import TracebackType
from typing import Literal, Protocol, cast

import asyncpg
import pytest
from examples.adoption.import_store import (
    DEFAULT_MAX_PAGE_BYTES,
    DEFAULT_MAX_PAGE_RECORDS,
    DEFAULT_MAX_PAGES,
    DEFAULT_MAX_RECORDS,
    DEFAULT_MAX_TOTAL_BYTES,
    CommitResult,
    ImportAlreadySettled,
    ImportBoundExceeded,
    ImportContractError,
    ImportCorruptStaging,
    ImportCursorMismatch,
    ImportEpochCASFailed,
    ImportIdentity,
    ImportMassDeleteRefused,
    ImportOrdinalGap,
    ImportPage,
    ImportRunActive,
    ImportRunNotOpen,
    ImportRunState,
    ImportSchemaError,
    ImportSourceIncomplete,
    ImportStagingDiverged,
    ImportStaleEpoch,
    ImportStore,
    ImportTransactionRequired,
    ImportWatermark,
    ImportWatermarkRegression,
    QuarantineReason,
    StageProgress,
)
from examples.adoption.provider import MAX_RECORD_REVISION, ProviderRecord

pytestmark = pytest.mark.integration

_FLOW = "generic-flow"
_SOURCE = "generic-feed"

type _Db = asyncpg.Pool | asyncpg.Connection


def _admin_statement(pg_dsn: str, statement: str) -> None:
    """Run one fixture-owner DDL statement on a private short-lived loop."""

    async def run() -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(statement)
        finally:
            await conn.close()

    asyncio.run(run())


@pytest.fixture
def import_schema(pg_dsn: str) -> Iterator[str]:
    """A bounded, fixture-owned scratch schema; never a TaskQ migration schema."""
    schema = "ap9imp_" + secrets.token_hex(6)
    try:
        yield schema
    finally:
        _admin_statement(pg_dsn, f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _sync_case(
    fn: Callable[..., Coroutine[object, object, object]],
) -> Callable[..., object]:
    """Run each scenario on a real loop without the wf-family G7 fixture hook.

    The suite appends the engine workflow G7 fixture to coroutine tests in
    ``test_wf_*`` modules. These are direct domain fixture controls with no
    workflow rows, so they are sync tests wrapping a real asyncio loop.
    """

    @wraps(fn)
    def wrapper(*args: object, **kwargs: object) -> object:
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


_ACK_LOSS_MESSAGE: str = (
    "test-only driver acknowledgement suppression after successful PostgreSQL COMMIT"
)


class _TransactionDelegate(Protocol):
    async def start(self) -> None: ...
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class _AckLossAfterCommit:
    """Test-only transaction-boundary acknowledgement suppressor.

    This is not an engine transaction and it does not fake a PostgreSQL result.
    ``__aexit__`` awaits the underlying actual asyncpg transaction COMMIT. Only
    after that real DBAPI call returns successfully does it raise
    ``asyncpg.InterfaceError`` so the application-facing caller observes a
    lost acknowledgement while the database commit is genuinely durable.

    If the body raises, the same exception is propagated after a real rollback.
    If the real COMMIT raises/rolls back, that real exception propagates and no
    synthetic lost-success is fabricated. Thus the injection seam cannot turn a
    failed commit into a claimed durable commit.
    """

    def __init__(self, transaction: _TransactionDelegate) -> None:
        self._transaction = transaction
        self._entered = False
        self._finished = False

    async def __aenter__(self) -> _AckLossAfterCommit:
        if self._entered or self._finished:
            raise RuntimeError("ack-loss transaction delegate cannot be reused")
        self._entered = True
        await self._transaction.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        if not self._entered or self._finished:
            raise RuntimeError("ack-loss transaction delegate was not entered")
        self._finished = True
        if exc_type is not None:
            await self._transaction.rollback()
            return False
        await self._transaction.commit()
        raise asyncpg.InterfaceError(_ACK_LOSS_MESSAGE)


def _record(
    index: int,
    *,
    revision: int = 0,
    content: str | None = None,
) -> ProviderRecord:
    return ProviderRecord(
        record_id=f"rec-{index:03d}",
        revision=revision,
        content=f"content-{index:03d}" if content is None else content,
    )


def _records(
    *indexes: int,
    revision: int = 0,
    content: str | None = None,
) -> tuple[ProviderRecord, ...]:
    return tuple(_record(index, revision=revision, content=content) for index in indexes)


def _identity(
    *,
    epoch: int = 1,
    source: str = _SOURCE,
    flow: str = _FLOW,
) -> ImportIdentity:
    return ImportIdentity(flow=flow, source=source, run_epoch=epoch)


def _page(
    *,
    cursor: str | None,
    next_cursor: str | None,
    records: Sequence[ProviderRecord],
    ordinal_start: int = 0,
) -> ImportPage:
    return ImportPage(
        cursor=cursor,
        next_cursor=next_cursor,
        has_more=next_cursor is not None,
        ordinal_start=ordinal_start,
        records=tuple(records),
    )


def _terminal_page(
    records: Sequence[ProviderRecord],
    *,
    cursor: str | None = None,
    ordinal_start: int = 0,
) -> ImportPage:
    return ImportPage(
        cursor=cursor,
        next_cursor=None,
        has_more=False,
        ordinal_start=ordinal_start,
        records=tuple(records),
    )


async def _create_pool(pg_dsn: str, *, max_size: int = 4) -> asyncpg.Pool:
    return await asyncpg.create_pool(pg_dsn, min_size=1, max_size=max_size)


async def _new_store(
    pool: asyncpg.Pool,
    schema: str,
    *,
    max_page_records: int = DEFAULT_MAX_PAGE_RECORDS,
    max_page_bytes: int = DEFAULT_MAX_PAGE_BYTES,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
    max_pages: int = DEFAULT_MAX_PAGES,
) -> ImportStore:
    store = ImportStore(
        schema=schema,
        max_page_records=max_page_records,
        max_page_bytes=max_page_bytes,
        max_records=max_records,
        max_total_bytes=max_total_bytes,
        max_pages=max_pages,
    )
    await store.initialize(pool)
    return store


async def _open_run(
    pool: asyncpg.Pool,
    store: ImportStore,
    identity: ImportIdentity,
    *,
    expected_current_epoch: int | None = None,
) -> None:
    async with pool.acquire() as conn, conn.transaction():
        await store.open_source_run(conn, identity, expected_current_epoch=expected_current_epoch)


async def _stage_pages(
    pool: asyncpg.Pool,
    store: ImportStore,
    identity: ImportIdentity,
    pages: Sequence[ImportPage],
    *,
    expected_current_epoch: int | None = None,
) -> StageProgress | None:
    async with pool.acquire() as conn, conn.transaction():
        await store.open_source_run(conn, identity, expected_current_epoch=expected_current_epoch)
        progress: StageProgress | None = None
        for page in pages:
            progress = await store.stage_page(conn, identity, page)
        return progress


async def _stage_terminal(
    pool: asyncpg.Pool,
    store: ImportStore,
    identity: ImportIdentity,
    records: Sequence[ProviderRecord],
    *,
    expected_current_epoch: int | None = None,
) -> None:
    await _stage_pages(
        pool,
        store,
        identity,
        [_terminal_page(records)],
        expected_current_epoch=expected_current_epoch,
    )


async def _commit(
    pool: asyncpg.Pool,
    store: ImportStore,
    identity: ImportIdentity,
    *,
    barrier: str = "logical-barrier-1",
    max_deletions: int = 0,
) -> CommitResult:
    async with pool.acquire() as conn, conn.transaction():
        return await store.commit_reconciliation(
            conn,
            identity,
            barrier=barrier,
            max_deletions=max_deletions,
        )


async def _quarantine(
    pool: asyncpg.Pool,
    store: ImportStore,
    identity: ImportIdentity,
    *,
    reason: QuarantineReason = "failed",
) -> ImportRunState:
    async with pool.acquire() as conn, conn.transaction():
        return await store.quarantine(conn, identity, reason=reason)


async def _read_run(db: _Db, store: ImportStore, identity: ImportIdentity) -> ImportRunState | None:
    if isinstance(db, asyncpg.Pool):
        async with db.acquire() as conn:
            return await store.read_run(conn, identity)
    return await store.read_run(db, identity)


async def _read_current(db: _Db, store: ImportStore, *, flow: str, source: str) -> int | None:
    if isinstance(db, asyncpg.Pool):
        async with db.acquire() as conn:
            return await store.read_current(conn, flow=flow, source=source)
    return await store.read_current(db, flow=flow, source=source)


async def _read_watermark(
    db: _Db, store: ImportStore, *, flow: str, source: str
) -> ImportWatermark | None:
    if isinstance(db, asyncpg.Pool):
        async with db.acquire() as conn:
            return await store.read_watermark(conn, flow=flow, source=source)
    return await store.read_watermark(db, flow=flow, source=source)


async def _mirror(
    db: _Db, schema: str, *, flow: str = _FLOW, source: str = _SOURCE
) -> list[tuple[str, int, str, int]]:
    rows = await db.fetch(
        f'SELECT record_id, revision, content, applied_epoch FROM "{schema}".'
        "adoption_import_mirror WHERE flow = $1 AND source = $2 ORDER BY record_id",
        flow,
        source,
    )
    return [
        (
            cast("str", row["record_id"]),
            int(cast("int | Decimal", row["revision"])),
            cast("str", row["content"]),
            cast("int", row["applied_epoch"]),
        )
        for row in rows
    ]


async def _history(
    db: _Db, schema: str, *, flow: str = _FLOW, source: str = _SOURCE
) -> list[tuple[int, str, str, str | None]]:
    rows = await db.fetch(
        f'SELECT run_epoch, barrier, kind, record_id FROM "{schema}".'
        "adoption_import_history WHERE flow = $1 AND source = $2 ORDER BY seq",
        flow,
        source,
    )
    return [
        (
            cast("int", row["run_epoch"]),
            cast("str", row["barrier"]),
            cast("str", row["kind"]),
            cast("str | None", row["record_id"]),
        )
        for row in rows
    ]


async def _count(
    db: _Db,
    schema: str,
    table: str,
    *,
    flow: str = _FLOW,
    source: str = _SOURCE,
) -> int:
    value = await db.fetchval(
        f'SELECT count(*) FROM "{schema}"."{table}" WHERE flow = $1 AND source = $2',
        flow,
        source,
    )
    return cast("int", value)


async def _install_failure_trigger(
    conn: asyncpg.Connection,
    schema: str,
    *,
    table: str,
    event: str,
) -> None:
    await conn.execute(
        f"""
        CREATE OR REPLACE FUNCTION "{schema}".fixture_statement_failure()
        RETURNS trigger LANGUAGE plpgsql AS $body$
        BEGIN
            RAISE EXCEPTION 'fixture statement-group failure';
        END
        $body$
        """
    )
    await conn.execute(
        f"CREATE TRIGGER fixture_statement_failure BEFORE {event} ON "
        f'"{schema}"."{table}" FOR EACH ROW '
        f'EXECUTE FUNCTION "{schema}".fixture_statement_failure()'
    )


async def _install_deferred_commit_failure(conn: asyncpg.Connection, schema: str) -> None:
    """A genuine server-side COMMIT failure: deferred constraint trigger."""
    await conn.execute(
        f"""
        CREATE OR REPLACE FUNCTION "{schema}".fixture_deferred_commit_failure()
        RETURNS trigger LANGUAGE plpgsql AS $body$
        BEGIN
            RAISE EXCEPTION 'fixture deferred commit failure';
        END
        $body$
        """
    )
    await conn.execute(
        f"CREATE CONSTRAINT TRIGGER fixture_deferred_commit_failure "
        f'AFTER INSERT ON "{schema}".adoption_import_settlement '
        f"DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        f'EXECUTE FUNCTION "{schema}".fixture_deferred_commit_failure()'
    )


@_sync_case
async def test_wf_import_store_bootstrap_is_idempotent_and_schema_scoped(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        await store.initialize(pool)
        table_names = await pool.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = $1 ORDER BY table_name",
            import_schema,
        )
        names = [cast("str", row["table_name"]) for row in table_names]
        assert names == [
            "adoption_import_current",
            "adoption_import_history",
            "adoption_import_mirror",
            "adoption_import_run",
            "adoption_import_settlement",
            "adoption_import_stage_page",
            "adoption_import_stage_record",
            "adoption_import_watermark",
        ]
        assert not any(name.startswith("job") or name.startswith("wf_") for name in names)

        for bad_schema in ("Public", "pg_fixture", "information_schema", "1bad", ""):
            with pytest.raises(ImportContractError):
                ImportStore(schema=bad_schema)

        bad_schema = "ap9impbad_" + secrets.token_hex(4)
        async with pool.acquire() as admin:
            await admin.execute(f'CREATE SCHEMA "{bad_schema}"')
            await admin.execute(
                f'CREATE TABLE "{bad_schema}".adoption_import_run (flow text NOT NULL)'
            )
        try:
            bad_store = ImportStore(schema=bad_schema)
            with pytest.raises(ImportSchemaError):
                await bad_store.initialize(pool)
            async with pool.acquire() as admin:
                surviving = await admin.fetch(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = $1 ORDER BY table_name",
                    bad_schema,
                )
            assert [cast("str", row["table_name"]) for row in surviving] == ["adoption_import_run"]
        finally:
            async with pool.acquire() as admin:
                await admin.execute(f'DROP SCHEMA "{bad_schema}" CASCADE')
    finally:
        await pool.close()


@pytest.mark.parametrize(
    ("table", "constraint"),
    [
        ("adoption_import_stage_record", "adoption_import_record_pkey"),
        ("adoption_import_settlement", "adoption_import_settlement_run_key"),
        (
            "adoption_import_settlement",
            "adoption_import_settlement_history_sum_check",
        ),
    ],
)
@_sync_case
async def test_wf_import_store_existing_shape_rejects_missing_key_or_check(
    pg_dsn: str, import_schema: str, table: str, constraint: str
) -> None:
    """A complete-looking pre-existing table must still carry load-bearing keys/checks."""
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        async with pool.acquire() as conn:
            await conn.execute(
                f'ALTER TABLE "{import_schema}"."{table}" DROP CONSTRAINT "{constraint}"'
            )
        with pytest.raises(ImportSchemaError) as failure:
            await store.initialize(pool)
        message = str(failure.value)
        if constraint.endswith("_pkey"):
            assert "missing the primary key" in message
        elif constraint.endswith("_key"):
            assert "missing the unique key" in message
        else:
            assert "missing named CHECK constraint" in message
            assert constraint in message
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_existing_shape_rejects_weakened_history_sum_check(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        async with pool.acquire() as conn:
            await conn.execute(
                f'ALTER TABLE "{import_schema}".adoption_import_settlement '
                "DROP CONSTRAINT adoption_import_settlement_history_sum_check"
            )
            await conn.execute(
                f'ALTER TABLE "{import_schema}".adoption_import_settlement '
                "ADD CONSTRAINT adoption_import_settlement_history_sum_check CHECK (true)"
            )
        with pytest.raises(ImportSchemaError) as failure:
            await store.initialize(pool)
        assert "does not enforce" in str(failure.value)
        assert "history_count = upserted_count + deleted_count" in str(failure.value)
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_existing_shape_rejects_missing_history_identity(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        async with pool.acquire() as conn:
            await conn.execute(
                f'ALTER TABLE "{import_schema}".adoption_import_history '
                "ALTER COLUMN seq DROP IDENTITY"
            )
        with pytest.raises(ImportSchemaError) as failure:
            await store.initialize(pool)
        assert "history identity" in str(failure.value)
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_same_connection_visibility_and_rollback(
    pg_dsn: str, import_schema: str
) -> None:
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool_a, import_schema)
        identity = _identity()
        conn_a = await pool_a.acquire()
        try:
            transaction = conn_a.transaction()
            await transaction.start()
            await store.open_source_run(conn_a, identity, expected_current_epoch=None)
            page = _terminal_page(_records(0, 1))
            await store.stage_page(conn_a, identity, page)

            inside = await store.read_run(conn_a, identity)
            assert inside is not None
            assert inside.staged_pages == 1
            assert inside.staged_records == 2
            assert inside.state == "staging"

            async with pool_b.acquire() as conn_b:
                assert await store.read_run(conn_b, identity) is None
                assert await store.read_current(conn_b, flow=_FLOW, source=_SOURCE) is None
                assert (
                    await conn_b.fetchval(
                        f'SELECT count(*) FROM "{import_schema}".'
                        '"adoption_import_stage_record" WHERE flow = $1 AND source = $2',
                        _FLOW,
                        _SOURCE,
                    )
                    == 0
                )
            await transaction.rollback()
        finally:
            await pool_a.release(conn_a)

        async with pool_b.acquire() as conn_b:
            assert await store.read_run(conn_b, identity) is None
            assert await store.read_current(conn_b, flow=_FLOW, source=_SOURCE) is None
        assert await _count(pool_b, import_schema, "adoption_import_stage_record") == 0
        assert await _count(pool_b, import_schema, "adoption_import_run") == 0
    finally:
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_wf_import_store_commit_visible_only_after_caller_commit(
    pg_dsn: str, import_schema: str
) -> None:
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool_a, import_schema)
        epoch_one = _identity()
        await _stage_terminal(pool_a, store, epoch_one, _records(0, 1))
        first = await _commit(pool_a, store, epoch_one)
        assert first.replayed is False
        assert await _mirror(pool_b, import_schema) == [
            ("rec-000", 0, "content-000", 1),
            ("rec-001", 0, "content-001", 1),
        ]

        epoch_two = _identity(epoch=2)
        page_one = _page(cursor=None, next_cursor="p2", records=_records(0, content="changed"))
        page_two = _terminal_page(_records(2), cursor="p2", ordinal_start=1)
        await _stage_pages(
            pool_a,
            store,
            epoch_two,
            [page_one, page_two],
            expected_current_epoch=1,
        )
        conn_a = await pool_a.acquire()
        try:
            transaction = conn_a.transaction()
            await transaction.start()
            uncommitted = await store.commit_reconciliation(
                conn_a,
                epoch_two,
                barrier="logical-barrier-2",
                max_deletions=1,
            )
            assert uncommitted.replayed is False
            assert await _mirror(conn_a, import_schema, source=_SOURCE) == [
                ("rec-000", 0, "changed", 2),
                ("rec-002", 0, "content-002", 2),
            ]
            async with pool_b.acquire() as conn_b:
                assert (
                    await store.read_settlement(conn_b, epoch_two, barrier="logical-barrier-2")
                    is None
                )
                assert await _mirror(conn_b, import_schema, source=_SOURCE) == [
                    ("rec-000", 0, "content-000", 1),
                    ("rec-001", 0, "content-001", 1),
                ]
                watermark = await store.read_watermark(conn_b, flow=_FLOW, source=_SOURCE)
                assert watermark is not None
                assert watermark.applied_epoch == 1
            await transaction.rollback()
        finally:
            await pool_a.release(conn_a)

        async with pool_b.acquire() as conn_b:
            assert (
                await store.read_settlement(conn_b, epoch_two, barrier="logical-barrier-2") is None
            )
            assert await _mirror(conn_b, import_schema, source=_SOURCE) == [
                ("rec-000", 0, "content-000", 1),
                ("rec-001", 0, "content-001", 1),
            ]
            assert (
                await _count(conn_b, import_schema, "adoption_import_history", source=_SOURCE) == 2
            )

        second = await _commit(
            pool_a,
            store,
            epoch_two,
            barrier="logical-barrier-2",
            max_deletions=1,
        )
        assert second.replayed is False
        async with pool_b.acquire() as conn_b:
            assert (
                await store.read_settlement(conn_b, epoch_two, barrier="logical-barrier-2")
                is not None
            )
            assert await _mirror(conn_b, import_schema, source=_SOURCE) == [
                ("rec-000", 0, "changed", 2),
                ("rec-002", 0, "content-002", 2),
            ]
            assert (
                await _count(conn_b, import_schema, "adoption_import_history", source=_SOURCE) == 5
            )
            watermark = await store.read_watermark(conn_b, flow=_FLOW, source=_SOURCE)
            assert watermark is not None
            assert watermark.applied_epoch == 2
            assert watermark.applied_through_ordinal == 1
    finally:
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_wf_import_store_idempotent_settlement_and_divergent_barrier(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _stage_terminal(pool, store, identity, _records(0, 1))
        staged_only = await _read_run(pool, store, identity)
        assert staged_only is not None
        assert staged_only.state == "staging"
        assert staged_only.staged_complete is True
        assert staged_only.staged_records == 2
        assert await _mirror(pool, import_schema) == []
        assert await _history(pool, import_schema) == []
        assert await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE) is None
        assert await _count(pool, import_schema, "adoption_import_settlement") == 0
        first = await _commit(pool, store, identity, barrier="one")
        assert first.replayed is False
        settled_run = await _read_run(pool, store, identity)
        assert settled_run is not None
        assert settled_run.state == "settled"
        assert settled_run.settled_barrier == "one"
        assert settled_run.staged_pages == 1
        assert settled_run.staged_records == 2
        before_history = await _history(pool, import_schema)
        before_mirror = await _mirror(pool, import_schema)
        assert len(before_history) == 2
        assert len(before_mirror) == 2

        replay = await _commit(pool, store, identity, barrier="one")
        assert replay.replayed is True
        assert replay.settlement == first.settlement
        assert await _history(pool, import_schema) == before_history
        assert await _mirror(pool, import_schema) == before_mirror
        assert await _count(pool, import_schema, "adoption_import_settlement") == 1

        with pytest.raises(ImportAlreadySettled):
            await _commit(pool, store, identity, barrier="different")
        async with pool.acquire() as conn, conn.transaction():
            with pytest.raises(ImportAlreadySettled):
                await store.stage_page(conn, identity, _terminal_page(_records(9), ordinal_start=2))
        assert await _history(pool, import_schema) == before_history
        assert await _mirror(pool, import_schema) == before_mirror
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_exact_stage_replay_and_divergence(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _open_run(pool, store, identity)
        page_one = _page(cursor=None, next_cursor="c1", records=_records(0, 1))
        async with pool.acquire() as conn, conn.transaction():
            first_progress = await store.stage_page(conn, identity, page_one)
            replay_progress = await store.stage_page(conn, identity, page_one)
            assert replay_progress == first_progress
            assert first_progress.staged_records == 2
            assert first_progress.staged_pages == 1
            assert first_progress.staged_through_cursor == "c1"

        staged_run = await _read_run(pool, store, identity)
        assert staged_run is not None
        assert staged_run.state == "staging"
        assert staged_run.staged_records == 2
        assert staged_run.staged_through_cursor == "c1"
        assert await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE) is None
        assert await _mirror(pool, import_schema) == []
        assert await _history(pool, import_schema) == []

        duplicate_across_pages = _terminal_page(
            (_record(0), _record(2)), cursor="c1", ordinal_start=2
        )
        with pytest.raises(ImportStagingDiverged):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(conn, identity, duplicate_across_pages)
        assert await _count(pool, import_schema, "adoption_import_stage_record") == 2

        divergent_content = _page(
            cursor=None,
            next_cursor="c1",
            records=(_record(0, content="divergent"), _record(1)),
        )
        with pytest.raises(ImportStagingDiverged):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(conn, identity, divergent_content)
        divergent_cursor = _page(cursor="wrong", next_cursor="c1", records=_records(0, 1))
        with pytest.raises(ImportStagingDiverged):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(conn, identity, divergent_cursor)
        with pytest.raises(ImportCursorMismatch):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(
                    conn,
                    identity,
                    _page(
                        cursor="wrong",
                        next_cursor=None,
                        records=_records(2),
                        ordinal_start=2,
                    ),
                )
        with pytest.raises(ImportOrdinalGap) as gap:
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(
                    conn,
                    identity,
                    _terminal_page(_records(2), cursor="c1", ordinal_start=3),
                )
        assert gap.value.expected == 2
        assert gap.value.observed == 3

        async with pool.acquire() as conn, conn.transaction():
            complete = await store.stage_page(
                conn,
                identity,
                _terminal_page(_records(2), cursor="c1", ordinal_start=2),
            )
            assert complete.complete is True
            assert complete.staged_records == 3
        assert await _count(pool, import_schema, "adoption_import_stage_record") == 3
        assert await _count(pool, import_schema, "adoption_import_stage_page") == 2
        await _commit(pool, store, identity)
        assert await _mirror(pool, import_schema) == [
            ("rec-000", 0, "content-000", 1),
            ("rec-001", 0, "content-001", 1),
            ("rec-002", 0, "content-002", 1),
        ]
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_epoch_isolation_and_stale_refusal(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        old = _identity()
        async with pool.acquire() as conn, conn.transaction():
            await store.open_source_run(conn, old, expected_current_epoch=None)
            await store.stage_page(
                conn, old, _page(cursor=None, next_cursor="old", records=_records(0))
            )
        quashed = await _quarantine(pool, store, old)
        assert quashed.state == "quarantined"
        with pytest.raises(ImportRunNotOpen):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(
                    conn,
                    old,
                    _terminal_page(_records(0), cursor="old", ordinal_start=1),
                )
        with pytest.raises(ImportRunNotOpen):
            await _commit(pool, store, old, max_deletions=1)

        new = _identity(epoch=2)
        await _stage_terminal(pool, store, new, _records(7), expected_current_epoch=1)
        new_commit = await _commit(pool, store, new)
        assert new_commit.replayed is False
        assert await _mirror(pool, import_schema) == [("rec-007", 0, "content-007", 2)]
        assert await _count(pool, import_schema, "adoption_import_stage_record") == 2
        old_run = await pool.fetchval(
            f'SELECT staged_records FROM "{import_schema}".'
            '"adoption_import_run" WHERE flow = $1 AND source = $2 AND run_epoch = 1',
            _FLOW,
            _SOURCE,
        )
        assert old_run == 1

        with pytest.raises(ImportStaleEpoch):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(
                    conn,
                    old,
                    _terminal_page(_records(0), cursor="old", ordinal_start=1),
                )
        with pytest.raises(ImportStaleEpoch):
            await _commit(pool, store, old, max_deletions=1)
        re_quarantined = await _quarantine(pool, store, old)
        assert re_quarantined.state == "quarantined"
        assert await _mirror(pool, import_schema) == [("rec-007", 0, "content-007", 2)]

        with pytest.raises(ImportEpochCASFailed):
            await _open_run(pool, store, _identity(epoch=3), expected_current_epoch=1)
        with pytest.raises(ImportEpochCASFailed):
            await _open_run(pool, store, _identity(epoch=3), expected_current_epoch=None)
        with pytest.raises(ImportEpochCASFailed):
            await _open_run(pool, store, _identity(epoch=2), expected_current_epoch=2)
        assert await _read_current(pool, store, flow=_FLOW, source=_SOURCE) == 2
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_prior_active_run_blocks_new_epoch(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        first = _identity()
        await _open_run(pool, store, first)
        async with pool.acquire() as conn, conn.transaction():
            await store.stage_page(
                conn, first, _page(cursor=None, next_cursor="c1", records=_records(0))
            )
        with pytest.raises(ImportSourceIncomplete):
            await _commit(pool, store, first, max_deletions=0)
        with pytest.raises(ImportRunActive):
            await _open_run(pool, store, _identity(epoch=2), expected_current_epoch=1)
        assert await _read_current(pool, store, flow=_FLOW, source=_SOURCE) == 1
        await _quarantine(pool, store, first)
        await _open_run(pool, store, _identity(epoch=2), expected_current_epoch=1)
        assert await _read_current(pool, store, flow=_FLOW, source=_SOURCE) == 2
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_empty_import_noop_and_delete_protection(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        empty = _identity()
        await _stage_terminal(pool, store, empty, ())
        first = await _commit(pool, store, empty, max_deletions=0)
        assert first.replayed is False
        assert first.settlement.page_count == 1
        assert first.settlement.record_count == 0
        assert first.settlement.upserted_count == 0
        assert first.settlement.deleted_count == 0
        assert first.settlement.history_count == 0
        assert first.settlement.applied_through_ordinal == -1
        assert first.settlement.applied_cursor is None
        assert await _mirror(pool, import_schema) == []
        watermark = await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE)
        assert watermark is not None
        assert watermark.applied_epoch == 1
        assert watermark.applied_through_ordinal == -1
        replay = await _commit(pool, store, empty, max_deletions=0)
        assert replay.replayed is True
        assert replay.settlement == first.settlement

        second = _identity(epoch=2)
        await _stage_terminal(pool, store, second, _records(0, 1), expected_current_epoch=1)
        await _commit(pool, store, second, max_deletions=0)

        third = _identity(epoch=3)
        await _stage_terminal(pool, store, third, (), expected_current_epoch=2)
        with pytest.raises(ImportMassDeleteRefused) as refused:
            await _commit(pool, store, third, max_deletions=0)
        assert refused.value.observed == 2
        assert refused.value.limit == 0
        third_run = await _read_run(pool, store, third)
        assert third_run is not None
        assert third_run.state == "staging"
        assert third_run.staged_complete is True
        assert await _mirror(pool, import_schema) == [
            ("rec-000", 0, "content-000", 2),
            ("rec-001", 0, "content-001", 2),
        ]
        watermark = await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE)
        assert watermark is not None
        assert watermark.applied_epoch == 2
        assert await _count(pool, import_schema, "adoption_import_settlement") == 2

        emptied = await _commit(pool, store, third, max_deletions=2)
        assert emptied.replayed is False
        assert emptied.settlement.deleted_count == 2
        assert emptied.settlement.history_count == 2
        assert await _mirror(pool, import_schema) == []
        assert len(await _history(pool, import_schema)) == 4
        watermark = await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE)
        assert watermark is not None
        assert watermark.applied_epoch == 3
        assert watermark.applied_through_ordinal == -1
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_mass_delete_refusal_writes_nothing(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        first = _identity()
        await _stage_terminal(pool, store, first, _records(0, 1, 2, 3))
        await _commit(pool, store, first, max_deletions=0)
        baseline_history = await _history(pool, import_schema)
        baseline_mirror = await _mirror(pool, import_schema)
        assert len(baseline_history) == 4

        second = _identity(epoch=2)
        await _stage_terminal(pool, store, second, _records(0), expected_current_epoch=1)
        async with pool.acquire() as conn, conn.transaction():
            with pytest.raises(ImportMassDeleteRefused) as refused:
                await store.commit_reconciliation(conn, second, barrier="second", max_deletions=2)
            assert refused.value.observed == 3
            assert refused.value.limit == 2
            # The refusal happened before any domain write, so this same
            # transaction can be committed with the domain still untouched.
            assert await _mirror(conn, import_schema, source=_SOURCE) == baseline_mirror
            assert await _history(conn, import_schema, source=_SOURCE) == baseline_history
            watermark = await store.read_watermark(conn, flow=_FLOW, source=_SOURCE)
            assert watermark is not None and watermark.applied_epoch == 1
            assert (
                await conn.fetchval(
                    f'SELECT count(*) FROM "{import_schema}".'
                    '"adoption_import_settlement" WHERE flow = $1 AND source = $2 '
                    "AND run_epoch = 2",
                    _FLOW,
                    _SOURCE,
                )
                == 0
            )
        assert await _mirror(pool, import_schema) == baseline_mirror
        assert await _history(pool, import_schema) == baseline_history

        settled = await _commit(pool, store, second, barrier="second", max_deletions=3)
        assert settled.replayed is False
        assert settled.settlement.deleted_count == 3
        assert await _mirror(pool, import_schema) == [("rec-000", 0, "content-000", 2)]
        assert len(await _history(pool, import_schema)) == 8
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_corrupt_staging_is_refused_without_domain_writes(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)

        invalid_record = ProviderRecord.model_construct(record_id="", revision=-1, content="bad")
        invalid_page = ImportPage.model_construct(
            cursor=None,
            next_cursor=None,
            has_more=False,
            ordinal_start=0,
            records=(invalid_record,),
        )
        with pytest.raises(ImportContractError):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(conn, _identity(), invalid_page)

        variants = (
            "content",
            "record_bytes",
            "page_count",
            "page_digest",
            "run_digest",
            "missing_row",
            "record_id",
        )
        for variant in variants:
            source = f"corrupt-{variant}"
            identity = _identity(source=source)
            await _stage_terminal(pool, store, identity, _records(0, 1))

            async with pool.acquire() as conn:
                if variant == "content":
                    await conn.execute(
                        f'UPDATE "{import_schema}".adoption_import_stage_record '
                        "SET content = 'tampered' WHERE flow = $1 AND source = $2 "
                        "AND ordinal = 0",
                        _FLOW,
                        source,
                    )
                elif variant == "record_bytes":
                    await conn.execute(
                        f'UPDATE "{import_schema}".adoption_import_stage_record '
                        "SET payload_bytes = payload_bytes + 1 "
                        "WHERE flow = $1 AND source = $2 AND ordinal = 0",
                        _FLOW,
                        source,
                    )
                elif variant == "page_count":
                    await conn.execute(
                        f'UPDATE "{import_schema}".adoption_import_stage_page '
                        "SET record_count = record_count + 1 "
                        "WHERE flow = $1 AND source = $2",
                        _FLOW,
                        source,
                    )
                elif variant == "page_digest":
                    await conn.execute(
                        f'UPDATE "{import_schema}".adoption_import_stage_page '
                        "SET page_digest = repeat('0', 64) "
                        "WHERE flow = $1 AND source = $2",
                        _FLOW,
                        source,
                    )
                elif variant == "run_digest":
                    await conn.execute(
                        f'UPDATE "{import_schema}".adoption_import_run '
                        "SET staged_digest = repeat('0', 64) "
                        "WHERE flow = $1 AND source = $2",
                        _FLOW,
                        source,
                    )
                elif variant == "missing_row":
                    await conn.execute(
                        f'DELETE FROM "{import_schema}".adoption_import_stage_record '
                        "WHERE flow = $1 AND source = $2 AND ordinal = 1",
                        _FLOW,
                        source,
                    )
                elif variant == "record_id":
                    await conn.execute(
                        f'UPDATE "{import_schema}".adoption_import_stage_record '
                        "SET record_id = 'bad id with spaces' "
                        "WHERE flow = $1 AND source = $2 AND ordinal = 0",
                        _FLOW,
                        source,
                    )
                else:
                    raise AssertionError(f"unhandled variant {variant}")

            async with pool.acquire() as conn, conn.transaction():
                with pytest.raises(ImportCorruptStaging):
                    await store.commit_reconciliation(
                        conn, identity, barrier="corrupt", max_deletions=0
                    )
                assert await _mirror(conn, import_schema, source=source) == []
                assert await _history(conn, import_schema, source=source) == []
                assert await store.read_watermark(conn, flow=_FLOW, source=source) is None
                assert (
                    await conn.fetchval(
                        f'SELECT count(*) FROM "{import_schema}".'
                        '"adoption_import_settlement" WHERE flow = $1 AND source = $2',
                        _FLOW,
                        source,
                    )
                    == 0
                )
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_page_and_retained_bounds_are_finite_and_atomic(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        base_store = await _new_store(pool, import_schema)

        record_bound_page = _page(
            cursor=None,
            next_cursor="r1",
            records=_records(0, 1, 2, 3),
        )
        record_store = await _new_store(
            pool,
            import_schema,
            max_page_records=3,
            max_page_bytes=10_000,
            max_records=4,
            max_pages=5,
            max_total_bytes=10_000,
        )
        record_identity = _identity(source="bounds-records")
        await _open_run(pool, record_store, record_identity)
        with pytest.raises(ImportBoundExceeded) as record_error:
            async with pool.acquire() as conn, conn.transaction():
                await record_store.stage_page(conn, record_identity, record_bound_page)
        assert record_error.value.bound == "page_records"
        assert record_error.value.limit == 3
        assert record_error.value.observed == 4
        run = await _read_run(pool, record_store, record_identity)
        assert run is not None and run.staged_pages == 0

        byte_page = _page(
            cursor=None,
            next_cursor="b1",
            records=(
                _record(10, content="x" * 500),
                _record(11, content="y" * 500),
            ),
        )
        byte_store = await _new_store(
            pool,
            import_schema,
            max_page_records=2,
            max_page_bytes=max(1, byte_page.payload_bytes - 1),
            max_records=4,
            max_total_bytes=byte_page.payload_bytes,
            max_pages=5,
        )
        byte_identity = _identity(source="bounds-bytes")
        await _open_run(pool, byte_store, byte_identity)
        with pytest.raises(ImportBoundExceeded) as byte_error:
            async with pool.acquire() as conn, conn.transaction():
                await byte_store.stage_page(conn, byte_identity, byte_page)
        assert byte_error.value.bound == "page_bytes"
        assert byte_error.value.observed == byte_page.payload_bytes

        retained_store = await _new_store(
            pool,
            import_schema,
            max_page_records=2,
            max_page_bytes=10_000,
            max_records=3,
            max_pages=10,
            max_total_bytes=10_000,
        )
        retained_identity = _identity(source="bounds-retained")
        await _open_run(pool, retained_store, retained_identity)
        async with pool.acquire() as conn, conn.transaction():
            progress = await retained_store.stage_page(
                conn,
                retained_identity,
                _page(cursor=None, next_cursor="m1", records=_records(20, 21)),
            )
            assert progress.staged_records == 2
        with pytest.raises(ImportBoundExceeded) as retained_error:
            async with pool.acquire() as conn, conn.transaction():
                await retained_store.stage_page(
                    conn,
                    retained_identity,
                    _page(
                        cursor="m1",
                        next_cursor=None,
                        records=_records(22, 23),
                        ordinal_start=2,
                    ),
                )
        assert retained_error.value.bound == "records"
        assert retained_error.value.limit == 3
        progress = await _read_run(pool, retained_store, retained_identity)
        assert progress is not None
        assert progress.staged_records == 2
        assert progress.staged_pages == 1

        page_store = await _new_store(
            pool,
            import_schema,
            max_page_records=1,
            max_page_bytes=10_000,
            max_records=4,
            max_pages=2,
            max_total_bytes=10_000,
        )
        page_identity = _identity(source="bounds-pages")
        await _open_run(pool, page_store, page_identity)
        async with pool.acquire() as conn, conn.transaction():
            await page_store.stage_page(
                conn,
                page_identity,
                _page(cursor=None, next_cursor="p1", records=_records(30)),
            )
            await page_store.stage_page(
                conn,
                page_identity,
                _page(
                    cursor="p1",
                    next_cursor="p2",
                    records=_records(31),
                    ordinal_start=1,
                ),
            )
        with pytest.raises(ImportBoundExceeded) as pages_error:
            async with pool.acquire() as conn, conn.transaction():
                await page_store.stage_page(
                    conn,
                    page_identity,
                    _page(
                        cursor="p2",
                        next_cursor=None,
                        records=_records(32),
                        ordinal_start=2,
                    ),
                )
        assert pages_error.value.bound == "pages"
        assert pages_error.value.limit == 2

        total_page_one = _page(
            cursor=None,
            next_cursor="t1",
            records=(_record(40, content="a" * 100),),
        )
        total_page_two = _terminal_page(
            (_record(41, content="b" * 100),), cursor="t1", ordinal_start=1
        )
        total_limit = total_page_one.payload_bytes + total_page_two.payload_bytes - 1
        total_store = await _new_store(
            pool,
            import_schema,
            max_page_records=1,
            max_page_bytes=max(total_page_one.payload_bytes, total_page_two.payload_bytes),
            max_records=4,
            max_total_bytes=total_limit,
            max_pages=10,
        )
        total_identity = _identity(source="bounds-total")
        await _open_run(pool, total_store, total_identity)
        async with pool.acquire() as conn, conn.transaction():
            await total_store.stage_page(conn, total_identity, total_page_one)
        with pytest.raises(ImportBoundExceeded) as total_error:
            async with pool.acquire() as conn, conn.transaction():
                await total_store.stage_page(conn, total_identity, total_page_two)
        assert total_error.value.bound == "total_bytes"
        assert total_error.value.limit == total_limit
        assert (
            await _count(pool, import_schema, "adoption_import_stage_record", source="bounds-total")
            == 1
        )
        assert base_store.schema == import_schema
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_ordinary_rerun_watermark_and_cross_source_scope(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        first = _identity()
        await _open_run(pool, store, first)
        async with pool.acquire() as conn, conn.transaction():
            await store.stage_page(
                conn, first, _page(cursor=None, next_cursor="c1", records=_records(0, 1))
            )
            staged = await store.stage_page(
                conn,
                first,
                _terminal_page(_records(2), cursor="c1", ordinal_start=2),
            )
            assert staged.complete is True
            assert staged.staged_through_cursor is None
        before_commit_run = await _read_run(pool, store, first)
        assert before_commit_run is not None
        assert before_commit_run.state == "staging"
        assert before_commit_run.staged_records == 3
        assert await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE) is None
        await _commit(pool, store, first, max_deletions=0)

        sibling = _identity(source="sibling-feed")
        await _stage_terminal(pool, store, sibling, _records(9))
        await _commit(pool, store, sibling, max_deletions=0)

        second = _identity(epoch=2)
        await _open_run(pool, store, second, expected_current_epoch=1)
        async with pool.acquire() as conn, conn.transaction():
            await store.stage_page(
                conn,
                second,
                _page(
                    cursor=None,
                    next_cursor="d1",
                    records=(
                        _record(
                            1,
                            revision=MAX_RECORD_REVISION,
                            content="revision-updated",
                        ),
                    ),
                ),
            )
            await store.stage_page(
                conn,
                second,
                _terminal_page(_records(3), cursor="d1", ordinal_start=1),
            )
        settled = await _commit(pool, store, second, barrier="logical-barrier-2", max_deletions=2)
        assert settled.replayed is False
        assert settled.settlement.deleted_count == 2
        assert settled.settlement.applied_through_ordinal == 1
        assert settled.settlement.applied_cursor == "d1"

        assert await _mirror(pool, import_schema) == [
            ("rec-001", MAX_RECORD_REVISION, "revision-updated", 2),
            ("rec-003", 0, "content-003", 2),
        ]
        history = await _history(pool, import_schema)
        assert [(kind, record_id) for _epoch, _barrier, kind, record_id in history] == [
            ("upsert", "rec-000"),
            ("upsert", "rec-001"),
            ("upsert", "rec-002"),
            ("upsert", "rec-001"),
            ("upsert", "rec-003"),
            ("delete", "rec-000"),
            ("delete", "rec-002"),
        ]
        watermark = await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE)
        assert watermark is not None
        assert watermark.applied_epoch == 2
        assert watermark.applied_through_ordinal == 1
        assert watermark.applied_cursor == "d1"
        assert await _mirror(pool, import_schema, source="sibling-feed") == [
            ("rec-009", 0, "content-009", 1)
        ]
        sibling_watermark = await _read_watermark(pool, store, flow=_FLOW, source="sibling-feed")
        assert sibling_watermark is not None
        assert sibling_watermark.applied_epoch == 1

        with pytest.raises(ImportStaleEpoch):
            await _commit(pool, store, first, max_deletions=0)
        with pytest.raises(ImportEpochCASFailed):
            await _open_run(pool, store, _identity(epoch=2), expected_current_epoch=2)
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_watermark_tie_and_regression_fail_loudly(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        first = _identity()
        await _stage_terminal(pool, store, first, _records(0))
        await _commit(pool, store, first, max_deletions=0)
        baseline_mirror = await _mirror(pool, import_schema)
        baseline_history = await _history(pool, import_schema)
        assert len(baseline_mirror) == 1
        assert len(baseline_history) == 1

        second = _identity(epoch=2)
        await _stage_terminal(pool, store, second, _records(1), expected_current_epoch=1)
        async with pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{import_schema}".adoption_import_watermark '
                "SET applied_epoch = 2, applied_through_ordinal = 0, "
                "applied_cursor = 'forged' WHERE flow = $1 AND source = $2",
                _FLOW,
                _SOURCE,
            )
        with pytest.raises(ImportWatermarkRegression):
            async with pool.acquire() as conn, conn.transaction():
                await store.commit_reconciliation(conn, second, barrier="second", max_deletions=1)

        assert await _mirror(pool, import_schema) == baseline_mirror
        assert await _history(pool, import_schema) == baseline_history
        watermark = await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE)
        assert watermark is not None
        assert watermark.applied_epoch == 2
        assert watermark.applied_cursor == "forged"
        assert await _count(pool, import_schema, "adoption_import_settlement") == 1
        run = await _read_run(pool, store, second)
        assert run is not None and run.state == "staging"

        async with pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{import_schema}".adoption_import_watermark '
                "SET applied_epoch = 1, applied_through_ordinal = 0, "
                "applied_cursor = NULL WHERE flow = $1 AND source = $2",
                _FLOW,
                _SOURCE,
            )
        recovered = await _commit(pool, store, second, barrier="second", max_deletions=1)
        assert recovered.replayed is False
        assert recovered.settlement.record_count == 1
        assert await _mirror(pool, import_schema) == [("rec-001", 0, "content-001", 2)]
        assert len(await _history(pool, import_schema)) == 3
        watermark = await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE)
        assert watermark is not None
        assert watermark.applied_epoch == 2
        assert watermark.applied_through_ordinal == 0
    finally:
        await pool.close()


@pytest.mark.parametrize(
    ("table", "event"),
    [
        ("adoption_import_stage_record", "INSERT"),
        ("adoption_import_stage_page", "INSERT"),
        ("adoption_import_run", "UPDATE"),
    ],
)
@_sync_case
async def test_wf_import_store_stage_statement_group_rolls_back_atomically(
    pg_dsn: str, import_schema: str, table: str, event: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _open_run(pool, store, identity)
        async with pool.acquire() as conn:
            await _install_failure_trigger(conn, import_schema, table=table, event=event)
        with pytest.raises(asyncpg.PostgresError):
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(
                    conn,
                    identity,
                    _terminal_page(_records(0, 1)),
                )
        assert await _count(pool, import_schema, "adoption_import_stage_record") == 0
        assert await _count(pool, import_schema, "adoption_import_stage_page") == 0
        run = await _read_run(pool, store, identity)
        assert run is not None
        assert run.state == "staging"
        assert run.staged_pages == 0
        assert run.staged_records == 0
        assert run.staged_bytes == 0
        assert run.staged_complete is False
    finally:
        await pool.close()


@pytest.mark.parametrize(
    ("table", "event"),
    [
        ("adoption_import_mirror", "INSERT OR UPDATE"),
        ("adoption_import_mirror", "DELETE"),
        ("adoption_import_history", "INSERT"),
        ("adoption_import_watermark", "INSERT OR UPDATE"),
        ("adoption_import_settlement", "INSERT"),
        ("adoption_import_run", "UPDATE"),
    ],
)
@_sync_case
async def test_wf_import_store_settlement_statement_group_rolls_back_atomically(
    pg_dsn: str, import_schema: str, table: str, event: str
) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        first = _identity()
        await _stage_terminal(pool, store, first, _records(0, 1))
        await _commit(pool, store, first, max_deletions=0)
        baseline_mirror = await _mirror(pool, import_schema)
        baseline_history = await _history(pool, import_schema)
        assert len(baseline_mirror) == 2
        assert len(baseline_history) == 2

        second = _identity(epoch=2)
        await _stage_terminal(pool, store, second, _records(0), expected_current_epoch=1)
        async with pool.acquire() as conn:
            await _install_failure_trigger(conn, import_schema, table=table, event=event)
        with pytest.raises(asyncpg.PostgresError):
            async with pool.acquire() as conn, conn.transaction():
                await store.commit_reconciliation(conn, second, barrier="second", max_deletions=1)

        assert await _mirror(pool, import_schema) == baseline_mirror
        assert await _history(pool, import_schema) == baseline_history
        watermark = await _read_watermark(pool, store, flow=_FLOW, source=_SOURCE)
        assert watermark is not None
        assert watermark.applied_epoch == 1
        assert await _count(pool, import_schema, "adoption_import_settlement") == 1
        run = await _read_run(pool, store, second)
        assert run is not None
        assert run.state == "staging"
        assert run.staged_complete is True
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_terminate_before_commit_rolls_back_then_retry_once(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    writer: asyncpg.Connection | None = None
    admin: asyncpg.Connection | None = None
    transaction = None
    try:
        store = await _new_store(pool, import_schema)
        first = _identity()
        await _stage_terminal(pool, store, first, _records(0, 1))
        await _commit(pool, store, first, max_deletions=0)

        second = _identity(epoch=2)
        await _stage_terminal(pool, store, second, _records(0, 2), expected_current_epoch=1)
        writer = await asyncpg.connect(pg_dsn)
        admin = await asyncpg.connect(pg_dsn)
        writer_pid = writer.get_server_pid()
        transaction = writer.transaction()
        await transaction.start()
        await store.commit_reconciliation(writer, second, barrier="second", max_deletions=1)
        terminated = await admin.fetchval("SELECT pg_terminate_backend($1, 5000)", writer_pid)
        assert terminated is True
        with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
            await transaction.rollback()
        transaction = None

        async with pool.acquire() as conn:
            assert await store.read_settlement(conn, second, barrier="second") is None
            assert await _mirror(conn, import_schema) == [
                ("rec-000", 0, "content-000", 1),
                ("rec-001", 0, "content-001", 1),
            ]
            assert len(await _history(conn, import_schema)) == 2
            watermark = await store.read_watermark(conn, flow=_FLOW, source=_SOURCE)
            assert watermark is not None and watermark.applied_epoch == 1
            run = await store.read_run(conn, second)
            assert run is not None and run.state == "staging"

        recovered = await _commit(pool, store, second, barrier="second", max_deletions=1)
        assert recovered.replayed is False
        assert recovered.settlement.record_count == 2
        assert recovered.settlement.deleted_count == 1
        async with pool.acquire() as conn:
            assert await _mirror(conn, import_schema) == [
                ("rec-000", 0, "content-000", 2),
                ("rec-002", 0, "content-002", 2),
            ]
            assert len(await _history(conn, import_schema)) == 5
            assert await _count(conn, import_schema, "adoption_import_settlement") == 2
    finally:
        if transaction is not None:
            with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
                await transaction.rollback()
        if writer is not None:
            with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
                await writer.close()
        if admin is not None:
            with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
                await admin.close()
        await pool.close()


@_sync_case
async def test_wf_import_store_ack_loss_after_real_commit_replays_once(
    pg_dsn: str, import_schema: str
) -> None:
    """A real COMMIT succeeds; the application caller observes acknowledgement loss.

    The injection seam is test-local ``_AckLossAfterCommit`` around the actual
    asyncpg transaction used by the application call path. It awaits the real
    COMMIT first and raises ``asyncpg.InterfaceError`` only after that call
    returns. It does not fake rows, claim network severance, or mask a failed
    COMMIT. A fresh connection must see all application effects, and a fresh
    replay must return the same settlement without duplicate history.
    """
    pool = await _create_pool(pg_dsn)
    writer: asyncpg.Connection | None = None
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _stage_terminal(pool, store, identity, _records(0, 1))
        writer = await asyncpg.connect(pg_dsn)
        lost = 0
        try:
            async with _AckLossAfterCommit(writer.transaction()):
                await store.commit_reconciliation(writer, identity, barrier="one", max_deletions=0)
        except asyncpg.InterfaceError as exc:
            assert str(exc) == _ACK_LOSS_MESSAGE
            lost += 1
        assert lost == 1
        print(f"ACK_LOSS lost={lost}")

        async with pool.acquire() as conn:
            marker = await store.read_settlement(conn, identity, barrier="one")
            assert marker is not None
            assert await _mirror(conn, import_schema) == [
                ("rec-000", 0, "content-000", 1),
                ("rec-001", 0, "content-001", 1),
            ]
            assert len(await _history(conn, import_schema)) == 2
            assert await _count(conn, import_schema, "adoption_import_settlement") == 1
            run = await store.read_run(conn, identity)
            assert run is not None
            assert run.state == "settled"
            watermark = await store.read_watermark(conn, flow=_FLOW, source=_SOURCE)
            assert watermark is not None
            assert watermark.applied_epoch == 1

        replay = await _commit(pool, store, identity, barrier="one")
        assert replay.replayed is True
        assert replay.settlement == marker
        async with pool.acquire() as conn:
            assert len(await _history(conn, import_schema)) == 2
            assert await _mirror(conn, import_schema) == [
                ("rec-000", 0, "content-000", 1),
                ("rec-001", 0, "content-001", 1),
            ]
            assert await _count(conn, import_schema, "adoption_import_settlement") == 1
        print("ACK_LOSS durable_commit=settled mirror=2 history=2 settlement=1 replay=idempotent")
    finally:
        if writer is not None:
            with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
                await writer.close()
        await pool.close()


@_sync_case
async def test_wf_import_store_ack_suppression_does_not_mask_real_commit_failure(
    pg_dsn: str, import_schema: str
) -> None:
    """Negative control: a genuine COMMIT failure never becomes synthetic loss."""
    pool = await _create_pool(pg_dsn)
    writer: asyncpg.Connection | None = None
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _stage_terminal(pool, store, identity, _records(0, 1))
        async with pool.acquire() as conn:
            await _install_deferred_commit_failure(conn, import_schema)
        writer = await asyncpg.connect(pg_dsn)
        with pytest.raises(asyncpg.PostgresError) as failure:
            async with _AckLossAfterCommit(writer.transaction()):
                await store.commit_reconciliation(writer, identity, barrier="one", max_deletions=0)
        assert "fixture deferred commit failure" in str(failure.value)
        assert str(failure.value) != _ACK_LOSS_MESSAGE
        print(f"ACK_FAILURE real_commit_error={type(failure.value).__name__} synthetic_lost=0")

        async with pool.acquire() as conn:
            assert await store.read_settlement(conn, identity, barrier="one") is None
            assert await _mirror(conn, import_schema) == []
            assert await _history(conn, import_schema) == []
            assert await store.read_watermark(conn, flow=_FLOW, source=_SOURCE) is None
            assert await _count(conn, import_schema, "adoption_import_settlement") == 0
            run = await store.read_run(conn, identity)
            assert run is not None
            assert run.state == "staging"
            assert run.staged_complete is True
    finally:
        if writer is not None:
            with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
                await writer.close()
        await pool.close()


@_sync_case
async def test_wf_import_store_concurrent_epoch_cas_exactly_one_winner(
    pg_dsn: str, import_schema: str
) -> None:
    """Actual concurrent CAS: first epoch, valid next epoch, and skipped epoch."""
    pool = await _create_pool(pg_dsn, max_size=12)
    try:
        store = await _new_store(pool, import_schema)
        source = "cas-feed"

        async def open_candidate(epoch: int, expected: int | None) -> str:
            identity = ImportIdentity(flow=_FLOW, source=source, run_epoch=epoch)
            try:
                async with pool.acquire() as conn, conn.transaction():
                    await store.open_source_run(conn, identity, expected_current_epoch=expected)
                return "won"
            except ImportEpochCASFailed:
                return "cas-failed"
            except ImportRunActive:
                return "active-failed"

        async def run_rows(epoch: int) -> int:
            return cast(
                "int",
                await pool.fetchval(
                    f'SELECT count(*) FROM "{import_schema}".adoption_import_run '
                    "WHERE flow = $1 AND source = $2 AND run_epoch = $3",
                    _FLOW,
                    source,
                    epoch,
                ),
            )

        async def settle(identity: ImportIdentity, records: Sequence[ProviderRecord]) -> None:
            async with pool.acquire() as conn, conn.transaction():
                await store.stage_page(conn, identity, _terminal_page(records))
            await _commit(pool, store, identity, barrier=f"cas-{identity.run_epoch}")

        first_results = await asyncio.gather(*[open_candidate(1, None) for _ in range(6)])
        assert first_results.count("won") == 1
        assert set(first_results) == {"won", "cas-failed"}
        assert await _read_current(pool, store, flow=_FLOW, source=source) == 1
        assert await run_rows(1) == 1
        await settle(
            ImportIdentity(flow=_FLOW, source=source, run_epoch=1),
            _records(0, 1),
        )

        next_results = await asyncio.gather(*[open_candidate(2, 1) for _ in range(6)])
        assert next_results.count("won") == 1
        assert set(next_results) == {"won", "cas-failed"}
        assert await _read_current(pool, store, flow=_FLOW, source=source) == 2
        assert await run_rows(2) == 1
        await settle(
            ImportIdentity(flow=_FLOW, source=source, run_epoch=2),
            _records(0, 1),
        )

        mixed_results = await asyncio.gather(
            *[open_candidate(3, 2) for _ in range(3)],
            *[open_candidate(4, 3) for _ in range(3)],
        )
        assert mixed_results.count("won") == 1
        assert set(mixed_results) <= {"won", "cas-failed", "active-failed"}
        assert await _read_current(pool, store, flow=_FLOW, source=source) == 3
        assert await run_rows(3) == 1
        assert await run_rows(4) == 0
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_concurrent_same_source_writers_serialize(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    pool_a = await _create_pool(pg_dsn)
    pool_b = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _open_run(pool, store, identity)
        async with pool.acquire() as conn, conn.transaction():
            await store.stage_page(
                conn, identity, _page(cursor=None, next_cursor="c1", records=_records(0))
            )

        async def write_candidate(pool: asyncpg.Pool, content: str) -> StageProgress:
            async with pool.acquire() as conn, conn.transaction():
                return await store.stage_page(
                    conn,
                    identity,
                    _terminal_page(
                        (_record(1, content=content),),
                        cursor="c1",
                        ordinal_start=1,
                    ),
                )

        results = await asyncio.gather(
            write_candidate(pool_a, "from-a"),
            write_candidate(pool_b, "from-b"),
            return_exceptions=True,
        )
        successes = [result for result in results if not isinstance(result, BaseException)]
        failures = [result for result in results if isinstance(result, ImportStagingDiverged)]
        assert len(successes) == 1
        assert len(failures) == 1
        winner_content = await pool.fetchval(
            f'SELECT content FROM "{import_schema}".adoption_import_stage_record '
            "WHERE flow = $1 AND source = $2 AND ordinal = 1",
            _FLOW,
            _SOURCE,
        )
        assert winner_content in {"from-a", "from-b"}
        await _commit(pool, store, identity, max_deletions=0)
        assert await _mirror(pool, import_schema) == [
            ("rec-000", 0, "content-000", 1),
            ("rec-001", 0, cast("str", winner_content), 1),
        ]

        raced = _identity(source="commit-race")
        await _stage_terminal(pool, store, raced, _records(5, 6))

        async def commit_race(pool: asyncpg.Pool) -> CommitResult:
            async with pool.acquire() as conn, conn.transaction():
                return await store.commit_reconciliation(
                    conn, raced, barrier="race", max_deletions=0
                )

        race_results = await asyncio.gather(
            commit_race(pool_a),
            commit_race(pool_b),
        )
        assert {result.replayed for result in race_results} == {False, True}
        assert race_results[0].settlement == race_results[1].settlement
        async with pool.acquire() as conn:
            assert (
                await _count(conn, import_schema, "adoption_import_history", source="commit-race")
                == 2
            )
            assert (
                await _count(
                    conn, import_schema, "adoption_import_settlement", source="commit-race"
                )
                == 1
            )
    finally:
        await pool.close()
        await pool_a.close()
        await pool_b.close()


@_sync_case
async def test_wf_import_store_requires_caller_transaction(pg_dsn: str, import_schema: str) -> None:
    pool = await _create_pool(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _open_run(pool, store, identity)
        async with pool.acquire() as conn:
            assert conn.is_in_transaction() is False
            with pytest.raises(ImportTransactionRequired):
                await store.stage_page(conn, identity, _terminal_page(_records(0)))
            with pytest.raises(ImportTransactionRequired):
                await store.commit_reconciliation(conn, identity, barrier="one", max_deletions=0)
            with pytest.raises(ImportTransactionRequired):
                await store.quarantine(conn, identity, reason="failed")
            assert await _count(conn, import_schema, "adoption_import_stage_record") == 0
            assert await store.read_run(conn, identity) is not None
    finally:
        await pool.close()


@_sync_case
async def test_wf_import_store_closes_without_idle_transactions(
    pg_dsn: str, import_schema: str
) -> None:
    pool = await _create_pool(pg_dsn)
    admin = await asyncpg.connect(pg_dsn)
    try:
        store = await _new_store(pool, import_schema)
        identity = _identity()
        await _stage_terminal(pool, store, identity, _records(0))
        await _commit(pool, store, identity, max_deletions=0)
        await pool.close()
        idle = await admin.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() "
            "AND state = 'idle in transaction' AND pid <> pg_backend_pid()"
        )
        assert idle == 0
        idle_stage = await admin.fetchval(
            f'SELECT count(*) FROM "{import_schema}".adoption_import_stage_record'
        )
        assert idle_stage == 1
    finally:
        with contextlib.suppress(asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
            await pool.close()
        await admin.close()
