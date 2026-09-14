# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Red-team pins for age-based ``job_events`` retention.

``job_events`` has 16 INSERT sites and no DELETE anywhere in the package;
its only exit is the ``ON DELETE CASCADE`` when the leader prune removes a
terminal parent job. Two consequences, both confirmed:

* Events of a job that never terminates (the snooze loop re-nulls
  ``finished_at`` every cycle, so the terminality-keyed prune can never
  match it) live forever.
* Even for terminal jobs, event volume is bounded by job retention
  (30-90 days), not by anything proportional to what operators read.

The attached design (``docs/design/sql-hotpath-followups.md`` §2) settles
the shape: a leader-gated, batched ``DELETE`` by ``occurred_at`` —
``sweep_expired_events()`` in ``taskq/backend/_sweeps.py``, mirroring
``sweep_expired_results`` — driven by a worker-configurable
``event_retention_period`` / ``event_retention_batch_size`` under the
``TASKQ_`` prefix.

One carve-out is a hard constraint, not a preference: the
``kind='state_change' AND detail->>'reason' = 'lock_expired'`` slice of
``job_events`` is the crash-reclaim outbox that ``poll_reclaim_events``
and ``TaskQ.watch_reclaims()`` consume under a trailing-watermark
protocol. An age sweep that deletes that slice races the watermark and
silently corrupts crash reclamation — the failure this project's
bounded-writes rule was written about. The sweep must leave it alone.

These tests target the seam the design names. They fail today because no
such sweep or setting exists.
"""

from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_running_job, create_worker

pytestmark = pytest.mark.integration

_OLD = datetime.now(UTC) - timedelta(days=60)
_YOUNG = datetime.now(UTC) - timedelta(days=1)


_SweepFn = Callable[..., Coroutine[Any, Any, int]]


def _event_retention_sweep() -> _SweepFn:
    """The designed retention seam, or a descriptive failure.

    The design attached to the RCA names ``sweep_expired_events`` in
    ``taskq/backend/_sweeps.py``. If the fix lands the capability under a
    different name, update this driver — the assertions below are the
    contract, not the spelling.
    """
    from taskq.backend import _sweeps

    sweep = getattr(_sweeps, "sweep_expired_events", None)
    if sweep is None:
        pytest.fail(
            "no age-based job_events retention sweep exists. job_events has "
            "16 INSERT sites and zero DELETEs; its only exit is the ON "
            "DELETE CASCADE from a terminal parent, so events of a "
            "never-terminal job live forever. The attached design "
            "(docs/design/sql-hotpath-followups.md §2) names the seam: "
            "sweep_expired_events() in taskq/backend/_sweeps.py, a bounded "
            "batched DELETE by occurred_at, leader-gated per sweep tick."
        )
    return sweep


def test_event_retention_is_worker_configurable() -> None:
    """The retention window and batch size are worker settings under the
    ``TASKQ_`` prefix — the operator directive is a *configurable* sweep,
    not a hard-coded one."""
    missing = [
        name
        for name in ("event_retention_period", "event_retention_batch_size")
        # WorkerSettings is a dotenvmodel DotEnvConfig, not a pydantic
        # BaseModel: get_fields() is its introspection seam, mapping
        # name -> (type, FieldInfo).
        if name not in WorkerSettings.get_fields()
    ]
    assert not missing, (
        f"WorkerSettings has no {missing} — the retention sweep must be "
        "configurable via DotEnvConfig under the TASKQ_ prefix (the design "
        "settles TASKQ_EVENT_RETENTION_PERIOD, default ~7 days, 0 disables; "
        "and a batch size bounded like the other sweeps). "
        "grep -rn 'event_retention' src/taskq/settings.py finds nothing."
    )


async def _seed_events(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    *,
    occurred_at: datetime,
    count: int,
    kind: str = "state_change",
    detail: str = "{}",
) -> None:
    for _ in range(count):
        await conn.execute(
            f'INSERT INTO "{schema}".job_events (job_id, occurred_at, kind, detail) '
            "VALUES ($1, $2, $3, $4::jsonb)",
            job_id,
            occurred_at,
            kind,
            detail,
        )


async def _event_count(conn: asyncpg.Connection, schema: str, job_id: UUID) -> int:
    return await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1',
        job_id,
    )


async def test_event_retention_reclaims_old_events_of_live_jobs_in_bounded_batches(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """Old events are reclaimed even while their parent job is still live
    (non-terminal — the case the cascade can never reach), young events are
    kept, and one call deletes at most one bounded batch."""
    schema = module_pg_schema.schema_name
    sweep = _event_retention_sweep()

    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        with_events=False,
    )

    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_OLD, count=5)
    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_YOUNG, count=1)

    first_pass: int = await sweep(
        clean_pg_conn,
        schema=schema,
        retention=timedelta(days=30),
        batch_size=2,
    )
    assert first_pass <= 2, (
        f"one retention call deleted {first_pass} rows with batch_size=2 — "
        "the sweep must delete in bounded batches (one short transaction "
        "per batch), not drain the whole backlog in one statement."
    )
    assert first_pass > 0

    drained = first_pass
    while drained:
        drained = await sweep(
            clean_pg_conn,
            schema=schema,
            retention=timedelta(days=30),
            batch_size=2,
        )

    remaining = await _event_count(clean_pg_conn, schema, job_id)
    assert remaining == 1, (
        f"expected only the young event to survive, found {remaining} rows. "
        "The age sweep must reclaim every event older than the retention "
        "window — including those of a job that is still 'running', whose "
        "rows the terminality-keyed prune can never reach."
    )


async def test_event_retention_preserves_the_reclaim_outbox_slice(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """``kind='state_change' AND reason='lock_expired'`` events are the
    crash-reclaim outbox (``poll_reclaim_events`` /
    ``TaskQ.watch_reclaims()``). However old they are, the age sweep must
    not delete them — racing the trailing watermark silently corrupts
    crash reclamation."""
    schema = module_pg_schema.schema_name
    sweep = _event_retention_sweep()

    worker_id = new_uuid()
    await create_worker(clean_pg_conn, schema, worker_id)
    job_id = await create_running_job(
        clean_pg_conn,
        schema,
        worker_id,
        lock_expires_at=datetime.now(UTC) + timedelta(minutes=5),
        with_events=False,
    )

    await _seed_events(
        clean_pg_conn,
        schema,
        job_id,
        occurred_at=_OLD,
        count=2,
        kind="state_change",
        detail='{"reason": "lock_expired"}',
    )
    await _seed_events(clean_pg_conn, schema, job_id, occurred_at=_OLD, count=3)

    # Drain fully — several bounded passes.
    while await sweep(
        clean_pg_conn,
        schema=schema,
        retention=timedelta(days=30),
        batch_size=100,
    ):
        pass

    remaining = await _event_count(clean_pg_conn, schema, job_id)
    assert remaining == 2, (
        f"expected the 2 reclaim-outbox events to survive, found {remaining} "
        "rows. The lock_expired slice of job_events is machine-read by the "
        "crash-reclaim outbox under a trailing-watermark protocol; an age "
        "sweep must carve it out explicitly, whatever happens to every "
        "other kind."
    )
