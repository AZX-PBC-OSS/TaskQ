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
from typing import Any, Literal, cast

import asyncpg

from taskq.backend._protocol import JobId
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
    flow: Any,
    pool: asyncpg.Pool,
    schema: str,
    *,
    input: object = None,
    key: str | None = None,
    until: Literal["terminal", "held"] = "terminal",
    max_ticks: int = 5000,
    execute: bool = True,
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
    """
    runner = FlowRunner(flow, pool, schema)
    claim = await runner.create_flow(input=input, run_key=key)
    outcome_raw = await runner.drive(
        claim.flow_id, until=until, max_ticks=max_ticks, execute=execute
    )
    # The drive's label vocabulary IS the Literal (terminal/held/max_ticks —
    # drive's own docstring); the cast names the boundary, never a lie.
    outcome = cast("Literal['terminal', 'held', 'max_ticks']", outcome_raw)
    result: object | None = None
    if outcome == "terminal" and flow.terminal is not None:
        result = await runner.result(claim.flow_id)
    return WorkflowRunResult(claim=claim, outcome=outcome, result=result)
