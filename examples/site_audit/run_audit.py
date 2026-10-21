"""Run the site-audit pipeline end to end — the happy story.

Plans the default corpus (nine sites, one flagged for a
sub-investigation), drives the run in-process, and prints the TYPED
success receipt — the exact aggregate the fan-in produced:

    AUDIT COMPLETE  planned=9  sensor=5  panel=4  investigations=1  failed=[]

Requires ``TASKQ_PG_DSN`` and ``TASKQ_SCHEMA_NAME`` (the drills'
``README`` shows the export lines).
"""

from __future__ import annotations

import asyncio
import os
import time

import asyncpg

import taskq.migrate
from taskq.workflows import FlowRunner

from .pipeline import AuditReport, DefaultCorpus, app


async def main() -> None:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(dsn, schema=schema, phase="post")
    pool = await asyncpg.create_pool(dsn)

    compiled = app.get("site_audit")
    compiled.validate()  # the checker-independent validator — clean by construction
    runner = FlowRunner(compiled, pool, schema)

    flow_id = (await runner.create_flow(input=DefaultCorpus.build())).flow_id
    started = time.monotonic()
    await runner.drive(flow_id, until="terminal")
    wall = time.monotonic() - started

    report = AuditReport.model_validate(await runner.result(flow_id))  # the typed receipt
    print(
        f"AUDIT COMPLETE  planned={report.planned}  sensor={report.sensor_audited}  "
        f"panel={report.panel_audited}  investigations={report.investigations}  "
        f"failed={report.failed}  wall={wall:.2f}s  run={flow_id}"
    )
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
