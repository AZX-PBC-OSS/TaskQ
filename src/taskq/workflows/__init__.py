"""TaskQ Workflows — the DAG engine (``taskq[flows]``).

``import taskq`` never imports this package. The
only sanctioned entry is a direct ``import taskq.workflows`` (or a
submodule) by a caller that opted into the ``taskq[flows]`` extra. Nothing
under ``src/taskq/`` outside this package may import ``taskq.workflows`` at
module scope — the import-discipline pin rejects a violation.

The engine is core-deps-only (tors, asyncpg, uuid_utils — all base
dependencies); the ``flows`` extra carries no requirements of its own and
exists as the operator-facing opt-in marker.

PUBLIC SURFACE IS SMALL ON PURPOSE (§7b's cut): the root re-exports
the AUTHORING verbs + the RUNNER + the PACKAGED RUN (``run`` — the
one-call create+drive+read door) + the USER TYPES. Everything else
(SWEEP internals, the finalize mechanics, the SQL statements, the wire
specs, the ledger claims, the progress plumbing) is an ENGINE INTERNAL:
import it from its submodule — the documented path — never from here.
Every root name is a protocol-pin tax paid forever; the internals keep
their homes without the tax:

* ``api._graph`` owns the recorder + the wiring verbs; ``api._app`` the
  app/decorators/compiled-workflow/channel; ``api._validate`` owns
  ``validate()``; ``api._mermaid`` owns the emission; ``api._run`` owns
  the packaged one-call run; ``api._runner`` +
  its concern modules (``_sql_runner``/``_runner_codec``/
  ``_runner_errors``/``_ctx``/``_ctx_wait``/``_runner_loop``/
  ``_runner_chain``/``_runner_ladder``/``_runner_exit`` — §7b's split)
  own create/drive/result; ``api._loop`` owns the loop verbs;
  ``_childflow`` owns the child flows (the spawn face + the join
  handle); the progress READ surface (``run_display``/``keyed_display``
  — the operator's display contract, the progress guide's taught
  import) is re-exported here from ``_progress_read`` (the LedgerWatch
  F7 fix: the docs' path said "private" while the docs blessed it —
  the public re-export is the one tax paid, the private module stays
  the home).
* ``chain`` owns the chain surface; ``context`` owns ``ctx.step``;
  ``definitions`` owns the registered-definition registry;
  ``ledger`` the step-ledger claim + the run-key arbiter; ``engine`` the
  finalize mechanics; ``_sweep`` the sweep arms; ``_types`` the wire
  specs; ``_progress``/``_progress_read``/``_emit`` the streaming +
  progress plumbing; ``_status`` the derivation.
"""

from taskq.workflows._childflow import (
    DEFAULT_SPAWN_MAX_DEPTH,
    ChildCancelled,
    ChildFailed,
    ChildFlowLostError,
    ChildHandle,
    ChildOk,
    ChildOutcome,
    ChildWorkflowUnknownError,
    SpawnDepthExceededError,
)
from taskq.workflows._policy import (
    CLEAR_POLICY,
    ExecutionPolicy,
    ExhaustionEvent,
    FiniteReservationCohort,
    RetryDeferralBudgetExceeded,
    RetrySpec,
    TerminalEffect,
    WorkflowPolicyError,
    WorkflowRetryOverride,
)
from taskq.workflows._progress_read import keyed_display, run_display
from taskq.workflows.api import (
    CompiledWorkflow,
    Exit,
    GateDecl,
    Promise,
    RouteArm,
    WorkflowApp,
    WorkflowBuildError,
    build,
    chain_source,
    gather,
    map_source,
    route,
    sink,
    step,
)
from taskq.workflows.api._ctx_timer import TimerWake
from taskq.workflows.api._ctx_wait import Expired, LoopWaitShapeError
from taskq.workflows.api._hitl import DeliveryResult, HitlClient
from taskq.workflows.api._hitl_listen import (
    Backfilled,
    HitlListener,
    HoldCreated,
    HoldEvent,
    HoldExpired,
    HoldResolved,
)
from taskq.workflows.api._loop import Done, Refine, loop
from taskq.workflows.api._progress_listen import (
    ProgressBackfilled,
    ProgressEvent,
    ProgressListener,
    ProgressUpdated,
)
from taskq.workflows.api._run import WorkflowRunResult, run
from taskq.workflows.api._runner import (
    FlowRunner,
    StepContext,
    WorkflowRunCancelledError,
    WorkflowRunError,
)
from taskq.workflows.api._runner_exit import cancel_workflow_run, retry_workflow_node
from taskq.workflows.chain import (
    DONE,
    Chain,
    Route,
    RouterNotTotal,
    Step,
    chain_fork,
    chain_start,
)
from taskq.workflows.definitions import FanInBoundExceededError
from taskq.workflows.ledger import RunClaim

__all__ = [
    "CLEAR_POLICY",
    "DEFAULT_SPAWN_MAX_DEPTH",
    "DONE",
    "Backfilled",
    "Chain",
    "ChildCancelled",
    "ChildFailed",
    "ChildFlowLostError",
    "ChildHandle",
    "ChildOk",
    "ChildOutcome",
    "ChildWorkflowUnknownError",
    "CompiledWorkflow",
    "DeliveryResult",
    "Done",
    "ExecutionPolicy",
    "ExhaustionEvent",
    "Exit",
    "Expired",
    "FanInBoundExceededError",
    "FiniteReservationCohort",
    "FlowRunner",
    "GateDecl",
    "HitlClient",
    "HitlListener",
    "HoldCreated",
    "HoldEvent",
    "HoldExpired",
    "HoldResolved",
    "LoopWaitShapeError",
    "ProgressBackfilled",
    "ProgressEvent",
    "ProgressListener",
    "ProgressUpdated",
    "Promise",
    "Refine",
    "RetryDeferralBudgetExceeded",
    "RetrySpec",
    "Route",
    "RouteArm",
    "RouterNotTotal",
    "RunClaim",
    "SpawnDepthExceededError",
    "Step",
    "StepContext",
    "TerminalEffect",
    "TimerWake",
    "WorkflowApp",
    "WorkflowBuildError",
    "WorkflowPolicyError",
    "WorkflowRetryOverride",
    "WorkflowRunCancelledError",
    "WorkflowRunError",
    "WorkflowRunResult",
    "build",
    "cancel_workflow_run",
    "chain_fork",
    "chain_source",
    "chain_start",
    "gather",
    "keyed_display",
    "loop",
    "map_source",
    "retry_workflow_node",
    "route",
    "run",
    "run_display",
    "sink",
    "step",
]
