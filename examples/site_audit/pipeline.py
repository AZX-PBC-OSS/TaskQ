# ruff: noqa: S608  # Why: the SQL carries only value parameters or a drill-owned identifier — no untrusted text reaches any statement.
"""The site-audit pipeline — the fan-out / branch / fan-in shape, end to end.

A fleet-audit run plans N sites, fans one audit job out per site (the
typed route forks a child per element), branches each site's work on its
type (sensor stations and solar panels take DIFFERENT arms — the route's
arms are keyed by the item's union member), caps each audit family's
dispatch cadence with its own rate-limit bucket (``rate_limits=`` on the
arm), fans the arm results back in at the derived join, and aggregates
the survivors into one typed report. A site whose audit fails its retry
ladder lands in the join's TYPED failure record (``on_failure="collect"``)
— the run succeeds with the failure named, never a silent swallow.

Sites whose calibration reading exceeds the flag threshold get a
SUB-INVESTIGATION: a second workflow run spawned from inside the audit
arm (``FlowRunner.create_flow`` with a per-site run key — idempotent via
the run key) that assesses and verifies, then joins its verdict back
into the site's audit result.

The wiring verbs and the validator come from the workflow engine
(``docs/guides/workflows.md``); the rate-limit buckets are the authoring
surface's own (``docs/guides/rate-limiting.md`` — the step/arm/map
``rate_limits=`` face, Postgres backend so the bucket is correct across
processes).

Run the happy path with ``python -m examples.site_audit.run_audit``
(needs ``TASKQ_PG_DSN`` and ``TASKQ_SCHEMA_NAME``; see the README).
The drills in ``examples/site_audit/drills/`` prove each face on a live
database.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Literal

import asyncpg
from pydantic import BaseModel

from taskq.ratelimit import TokenBucket
from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    route,
    step,
)

# ── the plan (the run's input) ────────────────────────────────────────────


class SiteSpec(BaseModel):
    """One site on the audit plan: what the planner hands the run."""

    site_id: str
    kind: Literal["sensor", "panel"]
    reading: float = 0.0  # sensor stations: the calibration reading
    tilt_deg: float = 0.0  # solar panels: the mounting tilt


class AuditInput(BaseModel):
    """The whole plan. Rides the run root as the flow input; bodies read
    it back through ``ctx.input`` (the run carries its input — never a
    closure)."""

    sites: list[SiteSpec] = []


# ── the fan-out items and the per-type results ────────────────────────────


class SiteSensor(BaseModel):
    """A sensor-station audit item (the route's first union member)."""

    kind: Literal["sensor"]
    site_id: str
    reading: float


class SitePanel(BaseModel):
    """A solar-panel audit item (the route's second union member)."""

    kind: Literal["panel"]
    site_id: str
    tilt_deg: float


class SensorAudit(BaseModel):
    """A sensor station's audit result (its own union member — the
    ``Literal`` discriminator is REQUIRED, not defaulted: the join's
    consumers decode the typed sum by the tag, and two members whose
    required fields stand in a subset relation decode by field-count
    luck)."""

    kind: Literal["sensor"]
    site_id: str
    calibration: float
    investigated: bool = False


class PanelAudit(BaseModel):
    """A solar panel's audit result."""

    kind: Literal["panel"]
    site_id: str
    passes: bool


class AuditReport(BaseModel):
    """The typed terminal aggregate — derived from the run's own collect
    (the survivors' sum + the plan residual); never hardcoded."""

    planned: int
    sensor_audited: int
    panel_audited: int
    investigations: int
    failed: list[str]


# ── the sub-investigation (the child-flow face) ───────────────────────────


class InvestigateInput(BaseModel):
    site_id: str
    reading: float


class InvestigateVerdict(BaseModel):
    site_id: str
    findings: int
    note: str


AUDIT_DELAY_S = float(os.environ.get("TASKQ_AUDIT_DELAY_S", "0") or 0)
"""Per-site audit latency, seconds. The drills set it so the fan-out is
still mid-flight when the drill's kill window lands; zero keeps the
happy path instant."""


READING_FLAG_THRESHOLD = 90.0
"""A sensor calibration reading above this flags the site for a
sub-investigation (a contrived threshold — the demo's arithmetic)."""


async def assess_body(ctx: StepContext) -> InvestigateVerdict:
    """The sub-flow's first step: read the flagged reading, count findings."""
    params = InvestigateInput.model_validate(ctx.input)
    findings = max(0, int(params.reading - READING_FLAG_THRESHOLD))
    return InvestigateVerdict(site_id=params.site_id, findings=findings, note="assessed")


async def verify_body(ctx: StepContext, verdict: InvestigateVerdict) -> InvestigateVerdict:
    """The sub-flow's second step: confirm the assessment (the two-step
    chain is the point — the child flow is a real flow, not a function
    call)."""
    return verdict.model_copy(update={"note": f"verified: {verdict.note}"})


INVESTIGATION_APP = WorkflowApp()


@INVESTIGATION_APP.workflow("site_investigation")
def site_investigation() -> Promise[object]:
    assessed = step(assess_body, key="assess")
    verified = step(verify_body, assessed, key="verify")
    return build(verified)


# ── the deps seam (the bodies' dependency door) ───────────────────────────


class AuditDeps:
    """What the audit bodies need beyond their data: the DSN + schema the
    sub-investigation runner uses. ONE instance, bound ONCE at the app
    door (``WorkflowApp(deps=...)``); bodies opt in by declaring the
    parameter. The execution-ledger is the drills' idempotency receipt:
    each body start inserts one row (the resume drill reads the counts)."""

    def __init__(self, dsn: str, schema: str, ledger: bool = False) -> None:
        self._dsn = dsn
        self._schema = schema
        self._ledger = ledger
        self._pool: asyncpg.Pool | None = None
        self._ledger_ready = False

    @property
    def schema(self) -> str:
        return self._schema

    async def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
        return self._pool

    async def record_execution(self, site_id: str, flow_id: str) -> None:
        """The drills' receipt: one row per body START, keyed by site."""
        if not self._ledger:
            return
        pool = await self.pool()
        if not self._ledger_ready:
            await pool.execute(
                f"CREATE TABLE IF NOT EXISTS {self._schema}.audit_executions ("
                "site_id text, flow_id text, pid int, at timestamptz DEFAULT now())"
            )
            self._ledger_ready = True
        await pool.execute(
            f"INSERT INTO {self._schema}.audit_executions (site_id, flow_id, pid) "
            "VALUES ($1, $2, $3)",
            site_id,
            flow_id,
            os.getpid(),
        )

    async def execution_counts(self) -> dict[str, int]:
        pool = await self.pool()
        rows = await pool.fetch(
            f"SELECT site_id, count(*) AS n FROM {self._schema}.audit_executions GROUP BY site_id"
        )
        return {r["site_id"]: r["n"] for r in rows}

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()


# ── the audit bodies (the typed route's arms) ─────────────────────────────


class SiteBusError(RuntimeError):
    """The audit arm's terminal failure — the drill's injected fault."""


async def plan_audit(ctx: StepContext, params: AuditInput) -> list[SiteSensor | SitePanel]:
    """The plan step: the run's OWN input is the truth (``create_flow(
    input=...)``); the wiring-declared batch is the default corpus. The
    returned list is what the route fans out — one child per element."""
    raw = ctx.input if ctx.input is not None else params.model_dump()
    plan = AuditInput.model_validate(raw)
    items: list[SiteSensor | SitePanel] = []
    for spec in plan.sites:
        if spec.kind == "sensor":
            items.append(SiteSensor(kind="sensor", site_id=spec.site_id, reading=spec.reading))
        else:
            items.append(SitePanel(kind="panel", site_id=spec.site_id, tilt_deg=spec.tilt_deg))
    return items


async def audit_sensor(ctx: StepContext, item: SiteSensor, deps: AuditDeps) -> SensorAudit:
    """The sensor arm: calibration + (above the threshold) a
    sub-investigation child flow that joins back into this result."""
    await deps.record_execution(item.site_id, str(ctx.flow_id))
    await asyncio.sleep(AUDIT_DELAY_S)
    if item.site_id.startswith("site-doomed"):
        raise SiteBusError(f"sensor bus unreachable for {item.site_id}")
    await ctx.progress(50, f"calibrating {item.site_id}", {"site": item.site_id})

    investigated = False
    calibration = round(item.reading * 0.98, 3)
    if item.reading > READING_FLAG_THRESHOLD:
        pool = await deps.pool()
        runner = FlowRunner(INVESTIGATION_APP.get("site_investigation"), pool, deps.schema)
        claim = await runner.create_flow(
            input=InvestigateInput(site_id=item.site_id, reading=item.reading),
            # the run key IS the idempotency: a re-run of this site's
            # audit answers existing-terminal with the prior verdict —
            # never a second silent investigation.
            run_key=f"site-investigation:{item.site_id}",
        )
        await runner.drive(claim.flow_id, until="terminal")
        verdict = InvestigateVerdict.model_validate(await runner.result(claim.flow_id))
        calibration = round(item.reading - verdict.findings, 3)
        investigated = True
    return SensorAudit(
        kind="sensor", site_id=item.site_id, calibration=calibration, investigated=investigated
    )


async def audit_panel(ctx: StepContext, item: SitePanel, deps: AuditDeps) -> PanelAudit:
    """The panel arm: the tilt check. A DIFFERENT body, a DIFFERENT
    result model — the route branches on the item's type, and only this
    arm's body ever sees a panel."""
    await deps.record_execution(item.site_id, str(ctx.flow_id))
    await asyncio.sleep(AUDIT_DELAY_S)
    await ctx.progress(50, f"inspecting tilt {item.site_id}", {"site": item.site_id})
    return PanelAudit(kind="panel", site_id=item.site_id, passes=item.tilt_deg < 35.0)


async def write_report(ctx: StepContext, audits: list[SensorAudit | PanelAudit]) -> AuditReport:
    """The fan-in's consumer: the typed sum of the SURVIVORS. A failed
    site contributes NO result item — its absence is the plan residual
    (``failed``), named here from the rows, never silently dropped."""
    raw = ctx.input if ctx.input is not None else AuditInput().model_dump()
    plan = AuditInput.model_validate(raw)
    planned_ids = {s.site_id for s in plan.sites}
    survivor_ids = {a.site_id for a in audits}
    return AuditReport(
        planned=len(planned_ids),
        sensor_audited=sum(1 for a in audits if isinstance(a, SensorAudit)),
        panel_audited=sum(1 for a in audits if isinstance(a, PanelAudit)),
        investigations=sum(1 for a in audits if isinstance(a, SensorAudit) and a.investigated),
        failed=sorted(planned_ids - survivor_ids),
    )


# ── the rate-limit buckets (the authoring surface's rate_limits=) ─────────


SENSOR_BUCKET = TokenBucket(
    "audit-sensor-calibration", capacity=5, refill_per_second=5.0, backend="postgres"
)
PANEL_BUCKET = TokenBucket(
    "audit-panel-inspection", capacity=3, refill_per_second=2.0, backend="postgres"
)
"""Per-branch buckets: each audit family's dispatch cadence is capped by
its own bucket (Postgres backend — correct across processes; the memory
backend is per-process and must not gate a fleet)."""


# ── the app ───────────────────────────────────────────────────────────────


def build_app(deps: AuditDeps, max_attempts: int = 3) -> WorkflowApp:
    """Bind ONE deps instance at the door and spell the graph.

    ``max_attempts`` is the audit ladder's per-node ceiling — leave
    headroom (the default 3): a fleet whose ceiling is already spent has
    no re-claim path left when a worker dies mid-fan-out."""
    app = WorkflowApp(deps=deps)

    @app.workflow("site_audit")
    def site_audit() -> Promise[object]:
        planned = step(plan_audit, AuditInput(), key="plan")
        # THE TYPED ROUTE: the arms are keyed by the item union's MEMBERS
        # (a route that is not total over the union is a BUILD refusal);
        # each arm body declares ITS member on the item param; the arms'
        # rate_limits are each branch's own bucket; and on_failure=
        # declares the join's failure policy — collect: a site whose
        # ladder exhausts fans in as a TYPED failure record and the
        # survivors' join still fires.
        audited = route(
            planned,
            {
                SiteSensor: RouteArm(body=audit_sensor, rate_limits=(SENSOR_BUCKET,)),
                SitePanel: RouteArm(body=audit_panel, rate_limits=(PANEL_BUCKET,)),
            },
            on_failure="collect",
            max_attempts=max_attempts,
        )
        return build(step(write_report, audited, key="report"))

    return app


def default_deps() -> AuditDeps:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    ledger = os.environ.get("TASKQ_AUDIT_EXEC_LEDGER") == "1"
    return AuditDeps(dsn, schema, ledger=ledger)


@dataclass(frozen=True, slots=True)
class DefaultCorpus:
    """The happy path's plan: eight sites, one flagged for a
    sub-investigation, nothing doomed."""

    sites: list[SiteSpec]

    @staticmethod
    def build() -> AuditInput:
        return AuditInput(
            sites=[
                SiteSpec(site_id=f"site-{i:03}", kind="sensor", reading=60.0 + i) for i in range(4)
            ]
            + [
                SiteSpec(site_id=f"site-{i:03}", kind="panel", tilt_deg=10.0 + i)
                for i in range(4, 8)
            ]
            + [SiteSpec(site_id="site-flagged", kind="sensor", reading=97.0)]
        )


# the module-level app (the import IS the registration — a worker process
# importing this module boots workflow-capable)
app = build_app(default_deps())
