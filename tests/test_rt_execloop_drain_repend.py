# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: DRAINING must re-pend rows the worker claimed but never started (E1).

Contract under attack: ``drain_local_queue_to_pending`` must re-pend every row
this worker CLAIMED but never started executing — the rows sitting in the
worker's ``local_queue`` — so the DRAINING phase actually stops consumers from
starting backlog jobs mid-shutdown.

Hypothesis (verified against the current tree): the drain UPDATE requires
``started_at IS NULL`` (src/taskq/worker/shutdown.py:106) while the dispatch
claim CTE stamps ``started_at = clock_timestamp()`` AT CLAIM
(src/taskq/backend/_dispatch_sql.py:191). Every local_queue row is therefore
running + locked + ``started_at IS NOT NULL``, so DRAINING matches zero rows.

This test claims rows through the REAL dispatch CTE
(``DISPATCH_STRICT_FIFO_SQL``) rather than fabricating rows by hand, so the
row shape under the drain predicate is exactly the producer's.

Fabricated-pin note (report-only; the file is not edited here):
``tests/test_shutdown_integration.py::test_ti4_drain_to_pending`` fabricates
the matched state by hand (``status='running'`` + ``locked_by_worker``, never
setting ``started_at``), so the existing pin never exercises the real claim
shape and passes while the real drain re-pends nothing.
"""

from datetime import timedelta
from typing import cast
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_claim_rows
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.pg import create_pending_job, seed_actors
from taskq.worker.deps import WorkerDeps
from taskq.worker.shutdown import drain_local_queue_to_pending

pytestmark = pytest.mark.integration


class _ConnCtx:
    """Async context manager yielding the single pinned connection."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _SingleConnPool:
    """Duck-typed asyncpg.Pool pinned to one live connection.

    Why: ``drain_local_queue_to_pending`` only needs
    ``acquire(timeout=...)`` yielding a connection with ``execute``; a
    one-connection wrapper runs the real drain SQL against the real asyncpg
    connection without constructing a pool.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self, timeout: float | None = None) -> _ConnCtx:
        return _ConnCtx(self._conn)


async def test_drain_re_pends_dispatch_claimed_rows(pg_dsn: str) -> None:
    """Rows claimed via the real dispatch CTE (started_at stamped at claim)
    must be re-pended by the drain helper — today the predicate matches 0."""
    schema = f"telo_{new_base62()}".lower()
    worker_id = new_uuid()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await apply_pending(conn, schema=schema)
        await seed_actors(conn, schema)
        job_ids: list[UUID] = [await create_pending_job(conn, schema) for _ in range(3)]

        # Claim exactly as the worker's producer does: the real dispatch CTE,
        # which stamps started_at = clock_timestamp() AT CLAIM.
        claimed = await dispatch_claim_rows(
            conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
            queues=["default"],
            limit_n=3,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=60),
        )
        assert len(claimed) == 3, f"setup: dispatch must claim 3 rows, got {len(claimed)}"

        # Probe: the local_queue row shape is running + locked + started_at
        # stamped at claim (src/taskq/backend/_dispatch_sql.py:191).
        probe = await conn.fetchrow(
            f"SELECT count(*) AS claimed_running, "
            f"count(*) FILTER (WHERE started_at IS NOT NULL) AS started_stamped "
            f"FROM \"{schema}\".jobs WHERE locked_by_worker = $1 AND status = 'running'",
            worker_id,
        )
        assert probe is not None
        assert probe["claimed_running"] == 3, (
            "setup: all 3 dispatched rows must be running+locked by this worker — "
            "this is exactly the claimed-but-unstarted local_queue backlog"
        )
        assert probe["started_stamped"] == 3, (
            "setup invariant: the dispatch claim CTE stamps "
            "`started_at = clock_timestamp()` at claim "
            "(src/taskq/backend/_dispatch_sql.py:191); if this fails the claim "
            "path changed and this attack must be re-verified"
        )

        settings = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": pg_dsn,
                "TASKQ_SCHEMA_NAME": schema,
                "TASKQ_CANCELLATION_GRACE_PERIOD": "30.0",
                "TASKQ_CLEANUP_GRACE_PERIOD": "10.0",
                "TASKQ_TERMINATION_GRACE_PERIOD": "60.0",
                "TASKQ_LOCK_LEASE": "45.0",
                "TASKQ_HEARTBEAT_INTERVAL": "5.0",
            }
        )
        # Why cast: _SingleConnPool is a duck-typed single-conn stand-in for
        # asyncpg.Pool on the drain path; the pinned contract is the drain
        # UPDATE's row selection, not pool fidelity.
        pool = cast("asyncpg.Pool", _SingleConnPool(conn))
        deps = WorkerDeps(
            settings=settings,
            dispatcher_pool=pool,
            heartbeat_pool=pool,
            worker_pool=pool,
            notify_conn=None,
            leader_conn=None,
        )

        drained = await drain_local_queue_to_pending(deps, worker_id)

        assert drained == 3, (
            "CONTRACT: drain_local_queue_to_pending must re-pend every row this "
            "worker claimed but never started executing (the local_queue backlog) "
            "so the DRAINING phase stops consumers from starting backlog jobs "
            "mid-shutdown. VIOLATION: the drain predicate requires "
            "`started_at IS NULL` (src/taskq/worker/shutdown.py:106) but the "
            "dispatch claim stamps `started_at = clock_timestamp()` at claim "
            "(src/taskq/backend/_dispatch_sql.py:191), so all 3 claimed-but-"
            f"unstarted rows stayed running+locked: drained={drained}."
        )

        rows = await conn.fetch(
            f'SELECT status, locked_by_worker FROM "{schema}".jobs WHERE id = ANY($1::uuid[])',
            job_ids,
        )
        assert len(rows) == 3
        for row, jid in zip(rows, job_ids, strict=True):
            assert row["status"] == "pending", (
                f"CONTRACT: claimed-but-unstarted job {jid} must be back to "
                f"pending after the drain; got status={row['status']!r} "
                "(still running+locked until lock-lease expiry — consumers "
                "gated only on shutdown_event can still start it mid-shutdown)"
            )
            assert row["locked_by_worker"] is None, (
                f"CONTRACT: the drain must clear the lock on re-pended job {jid}; "
                f"locked_by_worker={row['locked_by_worker']!r}"
            )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
