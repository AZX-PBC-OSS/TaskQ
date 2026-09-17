"""The worker bootstrap points the capacity gauges at its own registry.

``taskq.worker.active_jobs`` / ``taskq.worker.max_concurrency`` are the
OTel twins of the health socket's hand-rendered ``taskq_active_jobs``;
they report nothing until a worker is hosted, so a bootstrap that forgot
to point them at its deps would leave a real scrape without a capacity
series and no other test would notice.
"""

from __future__ import annotations

import asyncpg
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_base62
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.health import unique_health_sock_path
from taskq.testing.otel import collect_metrics
from taskq.worker.run import worker_main_async

pytestmark = pytest.mark.integration

_SCHEMA = f"twcg_{new_base62()}".lower()


def _gauge_values(reader: InMemoryMetricReader, name: str) -> list[float]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return [p.value for p in metric.data.data_points]  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


async def test_bootstrap_feeds_the_capacity_gauges(pg_dsn: str) -> None:
    """After a worker boots, the gauges read its registry and its configured
    max_concurrency; before, they read nothing."""
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(obs_mod.INSTRUMENTATION_NAME)
    meter.create_observable_gauge(
        "taskq.worker.active_jobs",
        callbacks=[otel_mod._observe_worker_active_jobs],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback against the production wiring is the point.
    )
    meter.create_observable_gauge(
        "taskq.worker.max_concurrency",
        callbacks=[otel_mod._observe_worker_max_concurrency],  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    )
    assert _gauge_values(reader, "taskq.worker.max_concurrency") == []

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE')
        await apply_pending(conn, schema=_SCHEMA)
    finally:
        await conn.close()
    settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": pg_dsn,
            "schema_name": _SCHEMA,
            "max_concurrency": "3",
            "health_socket_path": unique_health_sock_path("capacity_gauges"),
        }
    )
    try:
        code = await worker_main_async(
            settings,
            actor_registry={},
            cron_registry=[],
            until_idle=True,
            idle_settle_window=0.2,
            idle_poll_interval=0.1,
            idle_max_runtime=30.0,
        )
        assert code == 0
        # The source outlives the run, as the slot-pool source does: the
        # registry it points at is the worker's own, drained at exit.
        assert _gauge_values(reader, "taskq.worker.max_concurrency") == [3]
        assert _gauge_values(reader, "taskq.worker.active_jobs") == [0]
    finally:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA}" CASCADE')
        finally:
            await conn.close()
