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
    from taskq.backend._protocol import ConnLike
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


#: The post-fold `01.00.42_01` object set's cached per-schema verdict: a
#: bounded run's feature use refuses before any body, child, cursor, or park
#: write when ANY part of the object set is missing (the §2.1 whole-object
#: -set test — never a single-key or to_regclass-only check).
_EMIT_OBJECTS_OK: Final[set[str]] = set()

#: The folded arbiter's exact five-expression definition (the §2.1 catalog
#: fingerprint the preflight's `pg_get_indexdef` comparison requires).
_LEDGER_ARBITER_INDEXDEF: Final[str] = (
    "CREATE UNIQUE INDEX wf_step_ledger_claim_uniq ON {schema}.wf_step_ledger "
    "USING btree (flow_id, step_key, COALESCE(map_index, '-1'::integer), attempt, claim_epoch)"
)

#: The exact-park partial index's required definition (the probe, candidate,
#: and marker-sweep statements' own index).
_PARK_IDX_INDEXDEF: Final[str] = (
    "CREATE INDEX jobs_wf_emit_park_idx ON {schema}.jobs USING btree "
    "(((metadata->>'flow_id')::uuid), created_at, id) "
    "WHERE status = 'scheduled'::\"{schema}\".job_status "
    'AND (metadata @> \'{"blocking_reason":"backpressure"}\'::jsonb) '
    'AND (metadata @> \'{"wf_deferral":{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}\'::jsonb) '
    'AND (metadata @> \'{"wf_emit_backpressure":{"version":1}}\'::jsonb) '
    "AND (step_key IS NOT NULL) "
    "AND (step_key <> '__flow__'::text) "
    "AND (metadata ? 'flow_id')"
)

#: The pending-generation partial index's required definition.
_PENDING_IDX_INDEXDEF: Final[str] = (
    "CREATE INDEX wf_emit_capacity_pending_idx ON {schema}.wf_emit_capacity "
    "USING btree (flow_id) WHERE (wake_generation > handled_generation)"
)


async def verify_emit_capacity_objects(conn: ConnLike, schema: str) -> None:
    """The §2.1 whole-object-set catalog preflight (fail-closed, cached per
    schema).

    New bounded-emit code refuses before a body, child, cursor, or park
    write when the schema lacks ANY part of the post-fold ``01.00.42_01``
    object set. The checks inspect the relevant ``pg_attribute`` /
    ``pg_constraint`` / ``pg_index`` / ``pg_proc`` / ``pg_trigger`` facts
    and ``pg_get_indexdef`` text: mere ledger-key presence or
    ``to_regclass`` presence of one object is insufficient. Ordinary
    unbounded behavior never runs this and pays nothing."""
    if schema in _EMIT_OBJECTS_OK:
        return

    def _fail(detail: str) -> WorkflowWorkerCapabilityError:
        return WorkflowWorkerCapabilityError(
            "the schema lacks part of the 01.00.42_01 emit-capability object "
            f"set ({detail}); apply 01.00.42_01_pre_wf_step_ledger_claim_epoch.sql "
            "(the one folded PRE migration) before running bounded emit "
            "workflows. No body, child, cursor, or park write was made.",
            required_worker_schema=None,
        )

    # 1. the ledger claim_epoch column (pg_attribute).
    epoch_col = await conn.fetchval(
        """SELECT 1 FROM pg_attribute
           WHERE attrelid = ($1 || '.wf_step_ledger')::regclass
             AND attname = 'claim_epoch' AND NOT attisdropped""",
        f'"{schema}"',
    )
    if epoch_col is None:
        raise _fail("wf_step_ledger.claim_epoch is missing")

    # 2. the five-expression arbiter (pg_get_indexdef, exact expression set:
    # every arbiter member present, claim_epoch among them).
    arbiter = await conn.fetchval(
        "SELECT pg_get_indexdef(($1 || '.wf_step_ledger_claim_uniq')::regclass)", f'"{schema}"'
    )
    if arbiter is None or not all(
        member in arbiter
        for member in (
            "wf_step_ledger",
            "flow_id",
            "step_key",
            "map_index",
            "attempt",
            "claim_epoch",
        )
    ):
        raise _fail("wf_step_ledger_claim_uniq lacks the five-expression claim_epoch arbiter")

    # 3. wf_emit_capacity's typed columns and CHECK constraints.
    cols = await conn.fetch(
        """SELECT attname, format_type(atttypid, atttypmod) AS t
           FROM pg_attribute
           WHERE attrelid = ($1 || '.wf_emit_capacity')::regclass
             AND NOT attisdropped AND attnum > 0""",
        f'"{schema}"',
    )
    col_types = {r["attname"]: r["t"] for r in cols}
    for name, want in (
        ("flow_id", "uuid"),
        ("bound", "integer"),
        ("wake_generation", "bigint"),
        ("handled_generation", "bigint"),
    ):
        if col_types.get(name) != want:
            raise _fail(f"wf_emit_capacity.{name} is missing or mistyped")
    checks = await conn.fetchval(
        """SELECT count(*) FROM pg_constraint
           WHERE conrelid = ($1 || '.wf_emit_capacity')::regclass AND contype = 'c'""",
        f'"{schema}"',
    )
    if not checks or int(checks) < 2:
        raise _fail("wf_emit_capacity lacks its bound/generation CHECK constraints")

    # 4. the rotation singleton.
    rotation = await conn.fetchval(  # Why: the schema is the bundle's validated identifier; the singleton read takes no other value.
        "SELECT after_flow_id FROM " + f'"{schema}".wf_emit_intent_rotation WHERE id'
    )
    if rotation is None:
        raise _fail("wf_emit_intent_rotation's singleton row is missing")

    # 5. the pending-generation index (exact definition).
    pending = await conn.fetchval(
        "SELECT pg_get_indexdef(($1 || '.wf_emit_capacity_pending_idx')::regclass)",
        f'"{schema}"',
    )
    if pending is None or "wake_generation > handled_generation" not in pending:
        raise _fail("wf_emit_capacity_pending_idx is missing or mistyped")

    # 6. the guard function (pg_proc) and trigger (pg_trigger).
    fn = await conn.fetchval(
        """SELECT 1 FROM pg_proc p
           JOIN pg_namespace n ON n.oid = p.pronamespace
           WHERE n.nspname = $1 AND p.proname = 'wf_emit_park_guard'""",
        schema,
    )
    if fn is None:
        raise _fail("the wf_emit_park_guard function is missing")
    trig = await conn.fetchval(
        """SELECT 1 FROM pg_trigger
           WHERE tgrelid = ($1 || '.jobs')::regclass
             AND tgname = 'wf_emit_park_guard' AND NOT tgisinternal""",
        f'"{schema}"',
    )
    if trig is None:
        raise _fail("the wf_emit_park_guard trigger is missing on jobs")

    # 7. the exact-park partial index (exact definition via pg_get_indexdef).
    park_idx = await conn.fetchval(
        "SELECT pg_get_indexdef(($1 || '.jobs_wf_emit_park_idx')::regclass)",
        f'"{schema}"',
    )
    if (
        park_idx is None
        or "created_at, id" not in park_idx
        or "consume_budget" not in park_idx
        or "blocking_reason" not in park_idx
    ):
        raise _fail("jobs_wf_emit_park_idx is missing or does not match the exact-park predicate")

    _EMIT_OBJECTS_OK.add(schema)
