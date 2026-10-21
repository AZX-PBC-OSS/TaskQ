# ruff: noqa: S608  # Why: the SQL carries only value parameters or a drill-owned identifier — no untrusted text reaches any statement.
"""DRILL — the type-route proof: each entity type's arm ran, and ONLY its arm.

Six sites (three sensor stations, three solar panels) fan out through
the typed route. The drill reads the ROUTED CHILDREN'S OWN ROWS — the
``step_key`` names the arm that ran (``plan.item:…SiteSensor`` /
``plan.item:…SitePanel``) — and proves the receipts per type:

1. every sensor station's child ran the SENSOR arm (and only that arm);
2. every panel's child ran the PANEL arm (and only that arm);
3. both arms' survivors are in the typed sum; the report counts each
   type from the union (the discriminator decode — never field-count
   luck).

One command:

    python -m examples.site_audit.drills.type_route
"""

from __future__ import annotations

import json
import time

import asyncpg

from examples.site_audit.drills import _harness
from examples.site_audit.pipeline import AuditInput, AuditReport, SiteSpec, app
from taskq.workflows import FlowRunner


async def drill() -> None:
    schema = _harness.use_schema("route")
    await _harness.provision(schema)
    pool = await asyncpg.create_pool(_harness.dsn())
    runner = FlowRunner(app.get("site_audit"), pool, schema)

    plan = AuditInput(
        sites=[SiteSpec(site_id=f"site-{i:02}", kind="sensor", reading=60.0) for i in range(3)]
        + [SiteSpec(site_id=f"site-{i:02}", kind="panel", tilt_deg=20.0) for i in range(3, 6)]
    )
    flow_id = (await runner.create_flow(input=plan)).flow_id

    started = time.monotonic()
    await runner.drive(flow_id, until="terminal")
    wall = time.monotonic() - started

    rows = await pool.fetch(
        f'SELECT step_key, map_index, status, payload FROM "{schema}".jobs '
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'plan.item%' "
        "ORDER BY map_index",
        flow_id,
    )
    report = AuditReport.model_validate(await runner.result(flow_id))

    print(f"run={flow_id} wall={wall:.2f}s")
    sensor_rows = [r for r in rows if r["step_key"].endswith("SiteSensor")]
    panel_rows = [r for r in rows if r["step_key"].endswith("SitePanel")]
    for r in rows:
        kind = "sensor" if "SiteSensor" in r["step_key"] else "panel"
        site = json.loads(r["payload"])["wf_item"]["site_id"]
        print(f"  [{kind}] site={site}: {r['step_key']} -> {r['status']}")

    ok = (
        len(sensor_rows) == 3
        and len(panel_rows) == 3
        and all(r["status"] == "succeeded" for r in rows)
        and report.sensor_audited == 3
        and report.panel_audited == 3
        and report.failed == []
    )
    print(
        "TYPE-ROUTE PROOF OK — each type's arm ran and ONLY its arm "
        f"(sensor receipts={len(sensor_rows)}, panel receipts={len(panel_rows)}, "
        f"report sensor={report.sensor_audited} panel={report.panel_audited})"
        if ok
        else "TYPE-ROUTE PROOF FAILED — an arm ran its sibling's work, or a child died"
    )
    if not ok:
        raise SystemExit(1)
    await pool.close()


if __name__ == "__main__":
    _harness.run(drill)
