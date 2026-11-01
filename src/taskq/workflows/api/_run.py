"""THE PACKAGED RUN (the consumer report's ergonomic door — the
create-seam's cure 4): ``taskq.workflows.run(flow, pool, schema,
input=…, key=…)`` — the ONE-CALL surface that packages
:meth:`FlowRunner.create_flow` + :meth:`FlowRunner.drive` (+ the decoded
result read). The docs' pre-cure text taught
``workflows.run(flow, input, key=…)`` — AN API THAT DID NOT EXIST (the
doc lie). This is the REAL surface the docs now teach, and the pin runs
it end to end.

THE HONEST CLAIM RIDES IT: the returned :class:`WorkflowRunResult`
carries the typed :class:`~taskq.workflows.ledger.RunClaim` — ``created``
/ ``existing-running`` / ``existing-terminal`` — so a caller (the demo's
trigger, a cron fire) states the 202-vs-409 distinction from the CLAIM,
never from a second query. An existing-terminal replay is the
REFUSED-TO-REUSE verdict stated loudly: the run's result is returned as
the record's answer, the claim says ``existing-terminal``, and the
caller's re-run is the documented choice of a NEW key.

S-SIZED BY LAW: create + drive + read — nothing else. A fleet worker
replaces the drive loop (:meth:`FlowRunner.tick` / the worker-hosted
execution seam); this surface is the dev loop's and the tools' door.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, cast

import asyncpg
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from taskq.backend._protocol import JobId
from taskq.exceptions import SchemaNotMigratedError
from taskq.obs import safe_start_span
from taskq.workflows._obs import (
    record_run_started,
    record_run_terminal,
    run_span_attributes,
    run_span_name,
)
from taskq.workflows.api._app import CompiledWorkflow
from taskq.workflows.api._runner import FlowRunner
from taskq.workflows.ledger import RunClaim

__all__ = ["WorkflowRunResult", "run"]


@dataclass(frozen=True, slots=True)
class WorkflowRunResult:
    """The packaged run's outcome: the typed claim (the honest verdict),
    the drive's label, and — when the run reached a terminal verdict —
    the DECODED result (``None`` before it; ``held`` returns what the
    terminal node has, likely ``None``)."""

    claim: RunClaim
    outcome: Literal["terminal", "held", "max_ticks"]
    result: object | None

    @property
    def flow_id(self) -> JobId:
        """The run's id (the claim's own — the one-name law)."""
        return self.claim.flow_id

    @property
    def status(self) -> str:
        """The run's status AT CLAIM TIME (the row is the truth — a live
        run's status moves; the admin page and the ledger own it)."""
        return self.claim.status


async def run(
    flow: CompiledWorkflow,
    pool: asyncpg.Pool,
    schema: str,
    *,
    input: object = None,
    key: str | None = None,
    until: Literal["terminal", "held"] = "terminal",
    max_ticks: int = 5000,
    execute: bool = True,
    deps: object | None = None,
    priority: int | None = None,
) -> WorkflowRunResult:
    """THE ONE-CALL RUN: create the run (the typed claim — *key* is the
    run key, the G2 arbiter's rememberer), drive it to ``until``
    (``"terminal"`` by default; ``"held"`` stops at the human gate), and
    return the claim + the drive's outcome + the decoded result.

    *flow* is the COMPILED workflow (``app.get(name)`` — the compile is
    deterministic and registers the bodies it carries). ``execute=True``
    (the default) runs the bodies in THIS process (the dev loop's
    driver); ``execute=False`` is the orchestration-only pass — the
    fleet's workers execute the rows through the queue.

    *deps* is THE DEPS SEAM'S binding (the DI capability): ``None`` (the
    default) rides the compiled's own binding — the app's ONE instance,
    bound at ``WorkflowApp(deps=…)``; an explicit *deps* is the direct
    door's override (the runner-side binding upgrades the validation's
    view — the E12 contract reads the effective binding).

    THE RUN SPAN: this door is the RUN span's owner (``run {workflow}``
    — a root linked to the ambient trace, ended at the drive's stop;
    the contract and the attribute set in ``workflows/_obs.py``). The
    in-process drive's step executions are the span's children; a
    fleet-driven run's steps are the workers' own per-process roots,
    correlated by the ``taskq.flow_id`` attribute. Telemetry off → no
    spans, the run unchanged.

    *priority* is THE PER-CALL OVERRIDE (the wf-priority cure — the
    enqueue-time face the vanilla ``enqueue(priority=…)`` always had):
    the run's rows land at the value in the ``jobs.priority`` column —
    the dispatch claim's ``priority DESC`` (the same ORDER BY the
    vanilla rows obey) then serves an interactive run ahead of a deep
    default-0 backlog. The law's precedence: this override > the step's
    declaration > the actor handle's > the column default. ``None``
    (the default) = no override — the pre-cure byte-identical shape.
    The domain is the smallint column's (``check_priority_domain`` — the
    same refusal at every layer that accepts a priority).
    """
    from taskq.constants import check_priority_domain

    if priority is not None:
        check_priority_domain(priority, what="the run's priority override")
    runner = FlowRunner(flow, pool, schema, deps=deps if deps is not None else flow.deps)
    # THE RUN SPAN (the wf's own face of the vanilla obs law — the
    # contract lives in ``workflows/_obs.py``): a ROOT (``new_root=True``,
    # the producer-link pattern — linked, never parented, to the ambient
    # trace), created at the flow's FIRST claim — the run-key claim below
    # rides the span — and ended at the run's terminal (the drive's
    # stop). Telemetry off → the attributes' only reader is the span, so
    # nothing is built (the vanilla's gated-allocation law) and the span
    # is the non-recording no-op.
    current_span = trace.get_current_span()
    current_ctx = current_span.get_span_context()
    links = [trace.Link(current_ctx)] if current_ctx.is_valid else None
    with safe_start_span(
        run_span_name(flow.name),
        kind=SpanKind.INTERNAL,
        attributes=run_span_attributes(workflow=flow.name, run_key=key, priority=priority),
        links=links,
        new_root=True,
    ) as run_span:
        try:
            claim = await runner.create_flow(input=input, run_key=key, priority=priority)
        except asyncpg.exceptions.UndefinedTableError as exc:
            # THE FIRST CONTACT'S TRANSLATION (the teardown round's cure):
            # the packaged run on an unmigrated schema was the RAW driver
            # traceback; the client arm's typed SchemaNotMigratedError names
            # the remedy (the setup defect is actionable, not a raw
            # asyncpg face) — the packaged door now uses it too, the
            # original chained via __cause__.
            raise SchemaNotMigratedError(schema) from exc
        record_run_started(run_span, str(claim.flow_id))
        outcome_raw = await runner.drive(
            claim.flow_id, until=until, max_ticks=max_ticks, execute=execute
        )
        # The drive's label vocabulary IS the Literal (terminal/held/max_ticks —
        # drive's own docstring); the cast names the boundary, never a lie.
        outcome = cast("Literal['terminal', 'held', 'max_ticks']", outcome_raw)
        # THE TERMINAL'S TRUTH ON THE SPAN (gated — its only reader is the
        # span): the drive's label says the run terminalized, not WHICH
        # verdict the root row carries; one bounded primary-key read (the
        # same read the drive's own loop polled every tick) names it. A
        # held or max_ticks stop is not a terminal — nothing is read, the
        # outcome attribute below is the drive's own label.
        if outcome == "terminal" and run_span.is_recording():
            terminal_state = await runner._flow_status(claim.flow_id)  # pyright: ignore[reportPrivateUsage]  # Why: the runner's own status read — the private seam is this package's, and the span's terminal verdict is the read's only consumer (the gated-allocation law).
            record_run_terminal(run_span, terminal_state)
        run_span.set_attribute("taskq.outcome", outcome)
        result: object | None = None
        if outcome == "terminal" and flow.terminal is not None:
            result = await runner.result(claim.flow_id)
        return WorkflowRunResult(claim=claim, outcome=outcome, result=result)
