# ruff: noqa: S608  # Why: the SQL carries only value parameters or a drill-owned identifier — no untrusted text reaches any statement.
"""DRILL — the partial-failure proof.

One entity's audit is DOOMED (its id prefixes ``site-doomed`` — the arm
raises ``SiteBusUnreachable`` every attempt). The drill proves:

1. the rest of the fleet COMPLETES (9/10 survivors in the typed sum);
2. the failure lands TYPED — the join row's ``metadata.failures`` carries
   the full ``FailureInfo`` (error class, message, the attempt history,
   the policy that ran);
3. the aggregate reflects BOTH — the report's ``failed`` residual names
   the doomed site, the survivors' counts stand beside it;
4. the run SUCCEEDS — the envelope carries the failure, it is not a
   silent swallow and not a fleet-wide cascade.

Output: the receipt lines + the FailureInfo JSON. Run:

    python -m examples.site_audit.drills.partial_failure
"""

from __future__ import annotations

import json
import time

import asyncpg

from examples.site_audit.drills import _harness
from examples.site_audit.pipeline import AuditInput, AuditReport, SiteSpec, app
from taskq.workflows import FlowRunner


async def drill() -> None:
    schema = _harness.use_schema("partial")
    await _harness.provision(schema)
    pool = await asyncpg.create_pool(_harness.dsn())
    runner = FlowRunner(app.get("site_audit"), pool, schema)

    doomed = "site-doomed-00"
    plan = AuditInput(
        sites=[SiteSpec(site_id=f"site-{i:02}", kind="sensor", reading=60.0) for i in range(5)]
        + [SiteSpec(site_id=f"site-{i:02}", kind="panel", tilt_deg=20.0) for i in range(5, 10)]
        + [SiteSpec(site_id=doomed, kind="sensor", reading=61.0)]
    )
    flow_id = (await runner.create_flow(input=plan)).flow_id

    started = time.monotonic()
    await runner.drive(flow_id, until="terminal")
    wall = time.monotonic() - started

    report = AuditReport.model_validate(await runner.result(flow_id))

    # the join row's typed failure record (the collect's FailureInfo list)
    failures = await pool.fetchval(
        f"SELECT metadata->'failures' FROM \"{schema}\".jobs "
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'plan.join'",
        flow_id,
    )

    print(f"run={flow_id} wall={wall:.2f}s")
    print(f"REPORT {report.model_dump()}")
    print(f"FAILURE_INFO {json.dumps(json.loads(failures)[0]) if failures else 'none'}")

    survivors = report.sensor_audited + report.panel_audited
    assert report.failed == [doomed], report.failed
    assert survivors == 10, f"expected 10 survivors, got {survivors}"
    assert report.sensor_audited == 5 and report.panel_audited == 5
    info = json.loads(failures)[0] if failures else {}
    assert info["error"]["error_class"] == "SiteBusError", info
    assert info["policy"] == "collect", info
    print(
        "PARTIAL-FAILURE PROOF OK — the fleet completed "
        f"({survivors}/{report.planned}), the doomed site landed typed "
        f"({info['error']['error_class']}, {len(info['attempts'])} attempt(s) on the record), "
        "the run SUCCEEDED carrying the failure"
    )
    await pool.close()


if __name__ == "__main__":
    _harness.run(drill)
