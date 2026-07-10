"""Tests for the counter ``taskq.deadline_exceeded_sweep.jobs_failed``.

Covers per-actor counter increments from both backends:
- Unit (in-memory): two swept jobs across two distinct actors.
- Integration (PG): two swept jobs across two distinct actors against
  testcontainers Postgres, exercising the ``j.actor`` column added
  to ``_SWEEP_2_SQL RETURNING``.
"""

from datetime import UTC, datetime, timedelta

import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_base62, new_uuid
from taskq.backend import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.otel import counter_data_points

_START = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def metric_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the deadline-exceeded-swept counter.

    Replaces ``taskq.obs._deadline_exceeded_sweep_jobs_failed`` with a fresh
    counter backed by ``InMemoryMetricReader``. monkeypatch auto-restores
    the original.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())

    monkeypatch.setattr(
        otel_mod,
        "_deadline_exceeded_sweep_jobs_failed",
        new_meter.create_counter(
            "taskq.deadline_exceeded_sweep.jobs_failed",
            description="Jobs transitioned to failed by the deadline-exceeded sweep, labeled by actor.",
            unit="1",
        ),
    )

    return reader


def _enqueue_args(
    actor: str = "test_actor",
    schedule_to_close: datetime | None = None,
) -> EnqueueArgs:
    return make_enqueue_args(
        actor=actor,
        payload={"key": "value"},
        scheduled_at=_START,
        schedule_to_close=schedule_to_close,
    )


async def test_in_memory_sweep_increments_per_actor(
    metric_reader: InMemoryMetricReader,
) -> None:
    """Two swept jobs across two distinct actors; counter records one
    increment per swept job with the correct ``actor`` label."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    deadline = _START + timedelta(hours=1)
    now = _START + timedelta(hours=2)

    await backend.enqueue(_enqueue_args(actor="actor_alpha", schedule_to_close=deadline))
    await backend.enqueue(_enqueue_args(actor="actor_beta", schedule_to_close=deadline))

    count = await backend.deadline_sweep(now)
    assert count == 2

    dps = counter_data_points(metric_reader, "taskq.deadline_exceeded_sweep.jobs_failed")
    assert len(dps) == 2

    alpha_dp = [dp for dp in dps if dp.attributes == {"actor": "actor_alpha"}]
    beta_dp = [dp for dp in dps if dp.attributes == {"actor": "actor_beta"}]
    assert len(alpha_dp) == 1
    assert alpha_dp[0].value == 1
    assert len(beta_dp) == 1
    assert beta_dp[0].value == 1


async def test_in_memory_sweep_no_sweep_no_counter(
    metric_reader: InMemoryMetricReader,
) -> None:
    """When no jobs are swept, the counter has no data points."""
    backend = InMemoryBackend(clock=FakeClock(_START))
    now = _START + timedelta(hours=2)

    count = await backend.deadline_sweep(now)
    assert count == 0

    dps = counter_data_points(metric_reader, "taskq.deadline_exceeded_sweep.jobs_failed")
    assert len(dps) == 0


@pytest.mark.integration
async def test_pg_sweep_increments_per_actor(
    metric_reader: InMemoryMetricReader,
    pg_dsn: str,
) -> None:
    """Two swept jobs across two distinct actors against PG; counter
    records one increment per swept job with the correct ``actor`` label.

    This exercises the SQL change (``j.actor`` in ``RETURNING``) and
    the loop's actor-labeling logic.
    """
    import asyncpg

    from taskq.backend.postgres import PostgresBackend
    from taskq.migrate import apply_pending

    schema = f"tosc_{new_base62()}".lower()

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)

        deadline = datetime.now(UTC) - timedelta(seconds=10)
        now = datetime.now(UTC)

        await conn.execute(
            f"""INSERT INTO \"{schema}\".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, scheduled_at, schedule_to_close
            ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, 0, now(), $8)""",
            new_uuid(),
            "actor_alpha",
            "default",
            '{"key": "value"}',
            3,
            "transient",
            "pending",
            deadline,
        )
        await conn.execute(
            f"""INSERT INTO \"{schema}\".jobs (
                id, actor, queue, payload, max_attempts, retry_kind,
                status, priority, scheduled_at, schedule_to_close
            ) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, 0, now(), $8)""",
            new_uuid(),
            "actor_beta",
            "default",
            '{"key": "value"}',
            3,
            "transient",
            "pending",
            deadline,
        )

        count = await PostgresBackend.sweep_deadline_exceeded(conn, now, schema=schema)
        assert count == 2
    finally:
        await conn.close()

    dps = counter_data_points(metric_reader, "taskq.deadline_exceeded_sweep.jobs_failed")
    assert len(dps) == 2

    alpha_dp = [dp for dp in dps if dp.attributes == {"actor": "actor_alpha"}]
    beta_dp = [dp for dp in dps if dp.attributes == {"actor": "actor_beta"}]
    assert len(alpha_dp) == 1
    assert alpha_dp[0].value == 1
    assert len(beta_dp) == 1
    assert beta_dp[0].value == 1
