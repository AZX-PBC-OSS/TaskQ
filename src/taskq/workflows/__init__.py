"""TaskQ Workflows — the DAG engine (``taskq[flows]``).

THE IMPORT LAW (§16.1): ``import taskq`` NEVER imports this package. The
only sanctioned entry is a direct ``import taskq.workflows`` (or a
submodule) by a caller that opted into the ``taskq[flows]`` extra. Nothing
under ``src/taskq/`` outside this package may import ``taskq.workflows`` at
module scope — the import-discipline pin convicts a violation.

The engine is core-deps-only (tors, asyncpg, uuid_utils — all base
dependencies); the ``flows`` extra carries no requirements of its own and
exists as the operator-facing opt-in marker.

Module homes (concerns separate): ``engine`` owns the finalize mechanics
(tx1/tx2, the dispatch exclusion, the deadlock retry); ``_fork`` owns the
fork's atomic write set; ``_sweep`` owns the sweep arms (the lock-first
re-derive + fire, the outbox drain, the phantom reaper); ``_sql`` owns the
named statement constants; ``_types`` owns the specs/results and the
metadata wire shapes; ``ledger`` owns the step-ledger claim + the run-key
arbiter; ``context`` owns ``ctx.step``; ``definitions`` owns the
registered-definition registry; ``_capture`` owns the failure IO-capture
writer; ``_version`` owns the canonical code-version hash.
"""

from taskq.workflows._sweep import (
    SweepResult,
    drain_outbox,
    reap_phantom_ledger,
    sweep_join_rederive,
)
from taskq.workflows._types import (
    AbsorbingPolicy,
    ChildSpec,
    ConsumerBinding,
    DecrementHit,
    FailureInfo,
    FailurePolicy,
    FinalizeResult,
    FiredJoin,
    ForkSpec,
    JoinSpec,
    NodeSpec,
)
from taskq.workflows.context import WorkflowSteps
from taskq.workflows.definitions import (
    FAILURE_POLICIES,
    MAX_FAN_IN_PER_JOIN,
    DuplicateStepBodyError,
    DuplicateWorkflowError,
    StepBody,
    WorkflowDef,
    WorkflowRegistry,
    get_registry,
    resolve_step_body,
    validate_fork,
    validate_join_spec,
)
from taskq.workflows.engine import (
    DISPATCH_EXCLUSION_CLAUSE,
    DeadlockRetriesExhaustedError,
    fan_in_skip,
    finalize_node,
    insert_node,
    render_workflow_sql,
)
from taskq.workflows.ledger import (
    LedgerClaim,
    RunClaim,
    claim_step_ledger,
    insert_flow_run,
    memoized_step_result,
    run_idempotency_scope,
    step_idempotency_key,
    step_idempotency_scope,
)

__all__ = [
    "DISPATCH_EXCLUSION_CLAUSE",
    "FAILURE_POLICIES",
    "MAX_FAN_IN_PER_JOIN",
    "AbsorbingPolicy",
    "ChildSpec",
    "ConsumerBinding",
    "DeadlockRetriesExhaustedError",
    "DecrementHit",
    "DuplicateStepBodyError",
    "DuplicateWorkflowError",
    "FailureInfo",
    "FailurePolicy",
    "FinalizeResult",
    "FiredJoin",
    "ForkSpec",
    "JoinSpec",
    "LedgerClaim",
    "NodeSpec",
    "RunClaim",
    "StepBody",
    "SweepResult",
    "WorkflowDef",
    "WorkflowRegistry",
    "WorkflowSteps",
    "claim_step_ledger",
    "drain_outbox",
    "fan_in_skip",
    "finalize_node",
    "get_registry",
    "insert_flow_run",
    "insert_node",
    "memoized_step_result",
    "reap_phantom_ledger",
    "render_workflow_sql",
    "resolve_step_body",
    "run_idempotency_scope",
    "step_idempotency_key",
    "step_idempotency_scope",
    "sweep_join_rederive",
    "validate_fork",
    "validate_join_spec",
]
