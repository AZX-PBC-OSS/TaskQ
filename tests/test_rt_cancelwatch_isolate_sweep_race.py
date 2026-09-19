# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""Red-team: isolate_self racing the leader's Sweep 1 over the same rows.

``isolate_self``'s per-row sequence is: plain ``SELECT`` the running rows
locked by this worker (no locks taken), then for each row an
``UPDATE ... WHERE id = $1 AND status = 'running' AND locked_by_worker =
$2`` followed by an **unconditional** ``INSERT`` into ``job_attempts``
(``heartbeat.py``'s ``_inner``: the UPDATE's rowcount tag is discarded).
Sweep 1 transitions the same rows through a ``FOR UPDATE SKIP LOCKED``
snap and writes its own ``job_attempts`` row (outcome ``'crashed'``,
error_class ``'WorkerCrashed'``) in the same transaction - and
``job_attempts`` has ``PRIMARY KEY (job_id, attempt)``.

The guards make one transitioner a no-op - but only the UPDATE is
guarded.  Interleaving under attack: isolate's SELECT reads rows X and Y;
before isolate's per-row UPDATE of X, the leader's Sweep 1 reclaims X
(lease expired), writing (X, attempt) into ``job_attempts``; isolate's
guarded UPDATE of X then no-ops, and its unconditional attempt INSERT hits
the primary key - a **non-transient** constraint error that aborts
isolate's WHOLE transaction, so Y (still running, still locked by this
worker, never reclaimed) is never transitioned either.

Contract under test: losing the race on ONE row must not collapse the
isolation of the OTHERS - the sweep's transition of X stands, exactly one
attempt row exists for X, and Y is still isolated (re-pended for retry).
The benign ordering (sweep fully first, then isolate) is pinned green:
the guards serialise and isolate simply no longer sees the reclaimed row.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_uuid
from taskq.backend._sweeps import sweep_expired_locks
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_running_job, create_worker
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import isolate_self
from tests.conftest import _FakePool

pytestmark = pytest.mark.integration

_CANCEL_GRACE = timedelta(seconds=0)
_CLEANUP_GRACE = timedelta(seconds=10)
# Bounds every wait in this file so a wedged interleaving fails, never hangs.
_WAIT = 15.0


@pytest_asyncio.fixture(scope="module")
async def rt_schema(pg_dsn: str) -> AsyncIterator[tuple[str, str]]:
    """A random migrated schema on this module's database: ``(schema, dsn)``."""
    schema = f"tcw_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    yield schema, pg_dsn
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


def _isolate_deps(dsn: str, schema: str) -> WorkerDeps:
    return WorkerDeps(  # type: ignore[call-arg]
        settings=WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_LOCK_LEASE": "360",
                "TASKQ_TERMINATION_GRACE_PERIOD": "360",
                "TASKQ_CANCELLATION_GRACE_PERIOD": "0",
                "TASKQ_CLEANUP_GRACE_PERIOD": "10",
            }
        ),
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def _seed_two_running_jobs(
    conn: asyncpg.Connection, schema: str, worker_id: UUID
) -> tuple[UUID, UUID]:
    """X: lease long expired (sweep-eligible). Y: lease live (not eligible)."""
    x_id = await create_running_job(
        conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) - timedelta(seconds=600),
        max_attempts=3,
        retry_kind="transient",
    )
    y_id = await create_running_job(
        conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + timedelta(seconds=300),
        max_attempts=3,
        retry_kind="transient",
    )
    return x_id, y_id


async def _job_status(conn: asyncpg.Connection, schema: str, job_id: UUID) -> str:
    status = await conn.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    assert status is not None
    return str(status)


async def _attempt_rows(
    conn: asyncpg.Connection, schema: str, job_id: UUID
) -> list[asyncpg.Record]:
    return await conn.fetch(
        f'SELECT attempt, outcome, error_class FROM "{schema}".job_attempts '
        "WHERE job_id = $1 ORDER BY attempt",
        job_id,
    )


async def test_benign_ordering_sweep_first_then_isolate_is_serialised(
    rt_schema: tuple[str, str],
) -> None:
    """GREEN pin: the guards serialise the non-interleaved order.

    Sweep 1 fully commits its reclaim of X first; isolate then SELECTs only
    Y (X is no longer running) and transitions it - X untouched with
    exactly the sweep's one attempt row, Y isolated with exactly one.
    """
    schema, dsn = rt_schema
    worker_id = new_uuid()
    conn = await asyncpg.connect(dsn)
    try:
        await create_worker(conn, schema, worker_id)
        x_id, y_id = await _seed_two_running_jobs(conn, schema, worker_id)
        reclaimed = await sweep_expired_locks(conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema)
        assert reclaimed == 1, "fixture broken: only X was sweep-eligible"
        assert await _job_status(conn, schema, x_id) == "pending"

        shutdown = asyncio.Event()
        await asyncio.wait_for(
            isolate_self(_isolate_deps(dsn, schema), worker_id, shutdown),
            timeout=_WAIT,
        )
        assert shutdown.is_set()

        assert await _job_status(conn, schema, x_id) == "pending", (
            "Contract: the sweep's already-committed transition of X stands - "
            "isolate must not touch a row it no longer holds."
        )
        assert await _job_status(conn, schema, y_id) == "pending", (
            "Contract: isolate must transition the still-running row it does hold "
            "(retries remain → re-pended)."
        )
        x_attempts = await _attempt_rows(conn, schema, x_id)
        y_attempts = await _attempt_rows(conn, schema, y_id)
        assert len(x_attempts) == 1, (
            "Contract: exactly one attempt row for X - the sweep's. A second means "
            "isolate wrote a phantom attempt row for a row it lost."
        )
        assert x_attempts[0]["error_class"] == "WorkerCrashed"
        assert len(y_attempts) == 1, (
            "Contract: exactly one attempt row for Y - isolate's own write."
        )
        assert y_attempts[0]["error_class"] == "HeartbeatLost"
    finally:
        await conn.close()


class _GatedIsolateConn:
    """Real connection wrapper: parks isolate between its SELECT and the
    first per-row UPDATE, so the test can interleave Sweep 1 exactly inside
    the race window (isolate's plain SELECT holds no row locks)."""

    def __init__(self, conn: Any, parked: asyncio.Event, release: asyncio.Event) -> None:
        self._conn = conn
        self._parked = parked
        self._release = release
        self._execute_count = 0

    def transaction(self, **kwargs: object) -> Any:
        return self._conn.transaction(**kwargs)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        return await self._conn.fetch(sql, *args)

    async def execute(self, sql: str, *args: object) -> str:
        self._execute_count += 1
        if self._execute_count == 1:
            self._parked.set()
            await asyncio.wait_for(self._release.wait(), timeout=_WAIT)
        return await self._conn.execute(sql, *args)

    async def close(self) -> None:
        await self._conn.close()

    def terminate(self) -> None:
        self._conn.terminate()


async def test_isolate_survives_sweep_winning_one_row_mid_flight(
    rt_schema: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RED: a PK conflict on the sweep-won row must not abort the whole isolate.

    Interleaving: isolate has SELECTed X and Y when the leader's Sweep 1
    reclaims X (its lease was long expired) - X's guarded UPDATE no-ops,
    but isolate's unconditional attempt INSERT collides with the sweep's
    row on ``job_attempts``' PRIMARY KEY (job_id, attempt), a non-transient
    error that aborts isolate's entire transaction: Y - still running,
    still this worker's, never reclaimed - is left ``running`` with the
    worker exiting, instead of being isolated for retry.
    """
    schema, dsn = rt_schema
    worker_id = new_uuid()
    real_connect = asyncpg.connect
    sweep_conn = await real_connect(dsn)
    isolate_conn = await real_connect(dsn)
    parked = asyncio.Event()
    release = asyncio.Event()
    try:
        await create_worker(sweep_conn, schema, worker_id)
        x_id, y_id = await _seed_two_running_jobs(sweep_conn, schema, worker_id)

        async def fake_connect(dsn_arg: str, **_kwargs: object) -> Any:
            assert dsn_arg == dsn
            return _GatedIsolateConn(isolate_conn, parked, release)

        monkeypatch.setattr(asyncpg, "connect", fake_connect)

        shutdown = asyncio.Event()
        isolate_task = asyncio.create_task(
            isolate_self(_isolate_deps(dsn, schema), worker_id, shutdown)
        )
        # Parked between its SELECT (X and Y both read as running+mine) and
        # its first per-row UPDATE: the exact race window.
        await asyncio.wait_for(parked.wait(), timeout=_WAIT)

        # The leader's Sweep 1 wins X while isolate is parked.
        reclaimed = await sweep_expired_locks(
            sweep_conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
        )
        assert reclaimed == 1, "fixture broken: only X was sweep-eligible"

        release.set()
        await asyncio.wait_for(isolate_task, timeout=_WAIT)

        assert await _job_status(sweep_conn, schema, x_id) == "pending", (
            "Contract: the sweep's committed transition of X stands."
        )
        x_attempts = await _attempt_rows(sweep_conn, schema, x_id)
        assert len(x_attempts) == 1, (
            "Contract: exactly one attempt row for X - the sweep's; isolate lost "
            "the race and must not contribute a phantom row."
        )
        assert await _job_status(sweep_conn, schema, y_id) == "pending", (
            "Contract: losing the race on X must not collapse the isolation of Y - "
            "isolate's guarded UPDATE no-ops on X, so the unconditional attempt "
            "INSERT must not abort the whole transaction and leave Y running with "
            "no worker to finish it. Current behavior: the INSERT hits "
            "job_attempts' PRIMARY KEY (job_id, attempt) against the sweep's row "
            "(a non-transient constraint error) and Y was never isolated."
        )
        y_attempts = await _attempt_rows(sweep_conn, schema, y_id)
        assert len(y_attempts) == 1, (
            "Contract: Y's isolation writes exactly one attempt row when it is isolated."
        )
    finally:
        release.set()
        with contextlib.suppress(Exception):
            await isolate_conn.close()
        await sweep_conn.close()
