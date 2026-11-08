"""Internal workflow execution-schema capability primitives.

Schema level 1 contains the physical claim trigger and direct-driver refusal;
level 2 additionally implements guarded durable terminal effects. The boot
stamp below is the level this build actually implements, never an operator
override.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import asyncpg

from taskq.backend._protocol import JobId
from taskq.workflows.api._runner_errors import WorkflowRunError

if TYPE_CHECKING:
    from taskq.workflows.api._runner import FlowRunner
    from taskq.workflows.ledger import RunClaim

WORKFLOW_EXECUTION_SCHEMA: Final[int] = 2
#: The workflow execution-schema level THIS build implements. Worker boot
#: stamps exactly this value; it is raised only by the package that genuinely
#: implements the level's semantics, never by configuration.
WORKER_BOOT_WORKFLOW_SCHEMA: Final[int] = WORKFLOW_EXECUTION_SCHEMA
WORKER_CAPABILITY_SQLSTATE: Final[str] = "WF001"
REQUIRED_WORKER_SCHEMA_MAX: Final[int] = 2_147_483_647


def validate_required_worker_schema(value: object) -> int:
    """Validate the internal write door: a positive, exact, in-range integer.

    ``bool`` and integer-looking strings/floats are not coerced. ``None`` is
    intentionally not accepted here: it is the ordinary unguarded column value
    supplied by existing models, not an opted capability declaration.
    """
    if type(value) is not int:
        raise ValueError(
            "required_worker_schema must be an exact int; bool, strings, floats, "
            f"and other coercible values are refused (got {type(value).__name__})"
        )
    if not 1 <= value <= REQUIRED_WORKER_SCHEMA_MAX:
        raise ValueError(
            f"required_worker_schema must be between 1 and {REQUIRED_WORKER_SCHEMA_MAX}, "
            f"got {value}"
        )
    return value


class WorkflowWorkerCapabilityError(WorkflowRunError):
    """A guarded workflow body was not started behind an incapable driver.

    The persisted claim is unchanged when PostgreSQL raises WF001. The error
    carries identities as typed fields for worker/runner handling; its message
    gives the operator the actionable fleet remedy rather than suggesting a
    queue move or a hand-written worker metadata bypass.
    """

    def __init__(
        self,
        message: str,
        *,
        required_worker_schema: int | None,
        flow_id: JobId | None = None,
        job_id: JobId | None = None,
        worker_id: JobId | None = None,
    ) -> None:
        self.required_worker_schema = required_worker_schema
        self.flow_id = flow_id
        self.job_id = job_id
        self.worker_id = worker_id
        super().__init__(message)


def direct_capability_refusal(
    required_worker_schema: object,
    *,
    flow_id: JobId,
    job_id: JobId | None = None,
) -> WorkflowWorkerCapabilityError:
    required = validate_required_worker_schema(required_worker_schema)
    subject = f"workflow run {flow_id}" if job_id is None else f"workflow node {job_id}"
    return WorkflowWorkerCapabilityError(
        f"{subject} requires worker workflow_execution_schema >= {required}; "
        "the in-process FlowRunner deliberately refuses guarded execution. Apply "
        "the PRE capability migration and run the run's queues on upgraded TaskQ "
        "fleet workers whose actual boot metadata stamps that schema. Ordinary "
        "unguarded direct runs remain supported. No workflow body was started.",
        required_worker_schema=required,
        flow_id=flow_id,
        job_id=job_id,
    )


def postgres_capability_error(
    exc: asyncpg.PostgresError,
    *,
    flow_id: JobId | None = None,
    job_id: JobId | None = None,
) -> WorkflowWorkerCapabilityError | None:
    """Map only the trigger's stable SQLSTATE; preserve every other SQL error."""
    if exc.sqlstate != WORKER_CAPABILITY_SQLSTATE:
        return None
    return WorkflowWorkerCapabilityError(
        "PostgreSQL refused a workflow claim at the physical worker-capability "
        f"guard (SQLSTATE {WORKER_CAPABILITY_SQLSTATE}). Upgrade every worker "
        "serving the guarded run's queues, and keep worker registration owned by "
        "worker boot rather than editing metadata. No workflow body was started. "
        f"Server diagnostic: {exc}",
        required_worker_schema=None,
        flow_id=flow_id,
        job_id=job_id,
    )


async def create_guarded_flow(
    runner: FlowRunner,
    *,
    required_worker_schema: object,
    input: object = None,
    key: str | None = None,
    run_key: str | None = None,
    root_metadata_extra: dict[str, object] | None = None,
    priority: int | None = None,
) -> RunClaim:
    """Internal guarded-create door used by DDL tests and future producers.

    This is intentionally not a public workflow declaration or a new feature
    policy. The requirement is validated before opening the create transaction;
    root and initial body rows receive the same immutable value.
    """
    required = validate_required_worker_schema(required_worker_schema)
    return await runner.create_flow(
        input=input,
        key=key,
        run_key=run_key,
        root_metadata_extra=root_metadata_extra,
        priority=priority,
        _required_worker_schema=required,
    )
