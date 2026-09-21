"""End-to-end pin: the documented Prometheus endpoint serves ``taskq_*``
series after real worker activity against real Postgres.

The adopter red team's repro shape, held green: follow the documented
quick-start (``pip install taskq-py[prometheus]``, no ``OTEL_*`` env vars),
run real work, scrape ``GET /jobs/health/metrics`` as mounted by
``taskq ui serve`` - the served text must contain the ``taskq_*`` series
the shipped ``rules.yaml`` alert set references. Pre-fix it contained only
Python process defaults: 200, valid Prometheus text, zero ``taskq_*``
series, no error anywhere.

The subprocess gets a freshly migrated schema (the module PG fixtures) and
performs REAL worker activity through the production code paths - a real
enqueue (``client/_args.py`` records
``messaging.client.published.messages``) and a real dispatch round
(``backend/_dispatch_sql.py::dispatch_batch`` records
``taskq.dispatch.duration``) - then scrapes through the real FastAPI
router. Boot order inside the script is the shipped serve path's real
order: the router is created (auto-wiring the provider) at process start,
before any activity records, because OTel proxy instruments drop
pre-provider measurements by design.
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.otel]

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")

pytestmark = pytest.mark.integration

_SCRIPT = """
import asyncio
import os
from datetime import timedelta
from uuid import uuid4

import asyncpg
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

# Boot: exactly the ui-serve call in src/taskq/cli.py - router creation is
# process start on the shipped serve path (the FastAPI lifespan), which is
# where the provider auto-wiring happens.
from taskq.contrib.prometheus import create_metrics_router

router = create_metrics_router(None)
app = FastAPI()
app.include_router(router, prefix="/jobs/health")
client = TestClient(app)

from taskq import TaskQ, actor
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch

PG_DSN = os.environ["PROBE_PG_DSN"]
SCHEMA_NAME = os.environ["PROBE_SCHEMA"]


class _Payload(BaseModel):
    value: int = 1


@actor(name="prom_e2e_actor")
async def _probe_actor(_payload: _Payload) -> None:
    pass


async def _work() -> None:
    # Real worker activity through the production emitters: one enqueue...
    async with TaskQ(dsn=PG_DSN, schema=SCHEMA_NAME) as tq:
        await tq.enqueue(_probe_actor, _Payload(value=1))
    # ...and one dispatch round against the same schema.
    conn = await asyncpg.connect(PG_DSN)
    try:
        await dispatch_batch(
            conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=SCHEMA_NAME),
            queues=["default"],
            limit_n=5,
            worker_id=uuid4(),
            lock_lease=timedelta(seconds=60),
        )
    finally:
        await conn.close()


asyncio.run(_work())

resp = client.get("/jobs/health/metrics")
assert resp.status_code == 200, resp.status_code
text = resp.text

print("PUBLISHED_PRESENT:" + str("messaging_client_published_messages_total" in text))
print("DISPATCH_PRESENT:" + str("taskq_dispatch_duration_seconds_count" in text))
print("ANY_TASKQ:" + str(any(line.startswith("taskq_") for line in text.splitlines())))
"""


def test_served_metrics_contain_taskq_series_after_real_worker_activity(
    module_pg_schema: ModulePgSchema,
) -> None:
    """After a real enqueue and a real dispatch round, the scrape served by
    the router mounted exactly as ``taskq ui serve`` mounts it contains the
    ``taskq_*`` / ``messaging_*`` series the shipped alert rules reference -
    with no ``PrometheusMetricReader`` and no ``OTEL_*`` env vars anywhere,
    the exact documented quick-start state."""
    env = {
        # Scrub any ambient OTEL_*/TASKQ_* configuration: the documented
        # quick-start sets neither, and an inherited OTEL provider env var
        # would change which wiring branch runs.
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("OTEL_", "TASKQ_"))
    }
    env["PROBE_PG_DSN"] = module_pg_schema.pg_dsn
    env["PROBE_SCHEMA"] = module_pg_schema.schema_name

    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell - the current interpreter running this file's own literal script; the DSN/schema arrive via env, not argv.
        [sys.executable, "-c", _SCRIPT],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )
    assert result.returncode == 0, (
        f"subprocess failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    out: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.isupper():
            out[key] = value

    assert out.get("PUBLISHED_PRESENT") == "True", (
        "the enqueue's messaging_client_published_messages_total series is "
        f"absent from the served scrape: {result.stdout!r}"
    )
    assert out.get("DISPATCH_PRESENT") == "True", (
        "the dispatch round's taskq_dispatch_duration_seconds series is "
        f"absent from the served scrape: {result.stdout!r}"
    )
    assert out.get("ANY_TASKQ") == "True", (
        "no taskq_* series at all in the served scrape - the pre-fix "
        "silent-failure shape (200, valid text, only process defaults)"
    )
