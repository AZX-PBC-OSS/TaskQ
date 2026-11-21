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


def reset_emit_capability_cache(schema: str | None = None) -> None:
    """Invalidate the per-schema capability verdict (D4's negative
    controls' own seam, and the maintenance/migration paths' honest
    re-check): the cached OK is a process-lifetime fact about the
    catalog's SHAPE, and a schema whose objects were replaced after the
    first check must be re-verified rather than served the stale OK.
    ``None`` clears every schema."""
    if schema is None:
        _EMIT_OBJECTS_OK.clear()
    else:
        _EMIT_OBJECTS_OK.discard(schema)


#: The folded arbiter's EXACT definition (the §2.1 catalog fingerprint the
#: preflight's ``pg_get_indexdef`` comparison requires): the five-expression
#: claim arbiter, written in PostgreSQL's own ``pg_get_indexdef`` output
#: format (measured on PostgreSQL 18.6) with only the schema token
#: substituted. The comparison normalizes whitespace and quote characters
#: on both sides and requires FULL equality — a five-name substring check
#: accepted a wrong five-expression arbiter whose claim then failed 42P10.
_LEDGER_ARBITER_INDEXDEF: Final[str] = (
    "CREATE UNIQUE INDEX wf_step_ledger_claim_uniq ON {schema}.wf_step_ledger "
    "USING btree (flow_id, step_key, COALESCE((map_index)::integer, '-1'::integer), "
    "attempt, claim_epoch)"
)

#: The exact-park partial index's REQUIRED definition (the probe, candidate,
#: and marker-sweep statements' own index) — PostgreSQL's own
#: ``pg_get_indexdef`` output format, full-equality compared. A substring
#: check accepted a same-name wrong index whose probe then planned a Seq
#: Scan on jobs.
_PARK_IDX_INDEXDEF: Final[str] = (
    "CREATE INDEX jobs_wf_emit_park_idx ON {schema}.jobs USING btree "
    "((((metadata ->> 'flow_id'::text))::uuid), created_at, id) "
    "WHERE ((status = 'scheduled'::{schema}.job_status) "
    'AND (metadata @> \'{"blocking_reason": "backpressure"}\'::jsonb) '
    'AND (metadata @> \'{"wf_deferral": {"signal": "EmitBackpressure", '
    '"attempt": 0, "consume_budget": false}}\'::jsonb) '
    'AND (metadata @> \'{"wf_emit_backpressure": {"version": 1}}\'::jsonb) '
    "AND (step_key IS NOT NULL) "
    "AND (step_key <> '__flow__'::text) "
    "AND (metadata ? 'flow_id'::text))"
)

#: The pending-generation partial index's REQUIRED definition (PostgreSQL's
#: own ``pg_get_indexdef`` output format, full-equality compared).
_PENDING_IDX_INDEXDEF: Final[str] = (
    "CREATE INDEX wf_emit_capacity_pending_idx ON {schema}.wf_emit_capacity "
    "USING btree (flow_id) WHERE (wake_generation > handled_generation)"
)

#: The capacity table's REQUIRED CHECK constraints — (name, the exact
#: ``pg_get_constraintdef`` body as PostgreSQL renders it, measured on
#: PostgreSQL 18.6/18.7). Each is validated by NAME, contype, and FULL
#: normalized-body equality: the previous COUNT of CHECK constraints
#: accepted a same-name ``CHECK (true)`` pair — dropped and recreated
#: under their original names — whose loss then admitted ``bound = 0``
#: and ``wake_generation < handled_generation`` rows on real
#: PostgreSQL.
_CAPACITY_CHECKS: Final[tuple[tuple[str, str], ...]] = (
    ("wf_emit_capacity_bound_positive", "CHECK (bound > 0)"),
    ("wf_emit_capacity_generations_monotonic", "CHECK (wake_generation >= handled_generation)"),
)

#: The rotation singleton's REQUIRED CHECK — the migration's own
#: singleton invariant: a row may exist only with ``id`` true. Validated
#: with the same name + body rigour (a same-name ``CHECK (true)``
#: admits a second ``id = false`` row beside the singleton).
_ROTATION_CHECKS: Final[tuple[tuple[str, str], ...]] = (
    ("wf_emit_intent_rotation_singleton", "CHECK (id)"),
)


def _normalized_constraintdef(text: str) -> str:
    """The constraint comparison's normal form: whitespace runs collapsed
    and quote/parenthesis characters removed. Parenthesisation in a
    deparsed constraint body is grouping, never semantics (PostgreSQL
    renders ``CHECK (bound > 0)`` as ``CHECK ((bound > 0))``), and
    identifiers are quoted only when the name requires it — the index
    definitions' own normal form extended to the parens."""
    return " ".join("".join(char for char in text if char not in '()"').split())


def _contype(raw: object) -> str:
    """asyncpg decodes PostgreSQL's one-byte internal ``"char"`` type as
    ``bytes`` (``b'c'`` CHECK, ``b'p'`` PRIMARY KEY, ``b'u'`` UNIQUE);
    the comparisons want the letter."""
    if isinstance(raw, bytes):
        return raw.decode("ascii", errors="replace")
    return str(raw)


def _missing_check_constraint(
    present: dict[str, tuple[str, str | None]],
    required: tuple[tuple[str, str], ...],
    table: str,
) -> str | None:
    """The required CHECK constraints' refusal detail, or ``None`` when
    every one exists under its own name with its own body.

    Fail-closed on every wrong shape: a MISSING name, a name reused by
    a non-CHECK constraint (a UNIQUE/PK shadow — PostgreSQL only
    requires constraint-name uniqueness per table, so a dropped CHECK's
    name is free), a NULL/empty (unparseable) definition, or a
    same-name WRONG body is a refusal, never a pass. EXTRA constraints
    are allowed: an additional CHECK can only restrict writes further —
    the migration's own named set is what the preflight owes."""
    for name, body in required:
        entry = present.get(name)
        if entry is None:
            return f"{table} lacks its {name} CHECK constraint"
        contype, definition = entry
        if contype != "c":
            return f"{table}.{name} is not a CHECK constraint (contype {contype!r})"
        if definition is None or not definition.strip():
            return f"{table}.{name}'s definition is unparseable (NULL or empty)"
        if _normalized_constraintdef(definition) != _normalized_constraintdef(body):
            return f"{table}.{name} is not the required {body} — the catalog carries {definition!r}"
    return None


def _missing_primary_key(
    present: dict[str, tuple[str, str | None]],
    columns: tuple[str, ...],
    table: str,
) -> str | None:
    """The table's PRIMARY KEY refusal detail, or ``None`` when the PK
    is exactly over *columns* (the migration's own identity invariant —
    the capacity row's at-most-one-per-flow law, the rotation's
    singleton law)."""
    want = _normalized_constraintdef("PRIMARY KEY (" + ", ".join(columns) + ")")
    for _name, (contype, definition) in present.items():
        if contype != "p":
            continue
        if definition is not None and _normalized_constraintdef(definition) == want:
            return None
        return f"{table}'s PRIMARY KEY is not {want} (the catalog carries {definition!r})"
    return f"{table} lacks its PRIMARY KEY over ({', '.join(columns)})"


def _normalized_indexdef(text: str) -> str:
    """The exact-definition comparison's normal form: whitespace runs
    collapsed to single spaces and quote characters removed (PostgreSQL
    quotes identifiers only when the schema's name requires it; the
    declared constants carry the generic quoted shape)."""
    return " ".join(text.replace('"', "").split())


def _expected_park_indexdef(schema: str, *, status_type_visible: bool) -> str:
    """The exact-park partial index's expected ``pg_get_indexdef`` text for
    *schema*.

    PostgreSQL's ``ruleutils.c`` renders the ``jobs.status`` enum cast
    unqualified (``'scheduled'::job_status``) whenever ``"{schema}".job_status``
    is visible on the connection's ``search_path`` (``pg_type_is_visible``) —
    including a stock ``TASKQ_SCHEMA_NAME=public`` install on the default
    ``"$user", public`` path — and schema-qualified otherwise. The table
    target ``ON {schema}.jobs`` is always schema-qualified by
    ``pg_get_indexdef`` regardless of ``search_path``.
    """
    status_type = "job_status" if status_type_visible else f'"{schema}".job_status'
    return _PARK_IDX_INDEXDEF.replace("{schema}.job_status", status_type).replace(
        "{schema}", f'"{schema}"'
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

    # 2. the five-expression arbiter (pg_get_indexdef, EXACT definition
    # equality — D4: a five-NAME substring check accepted a wrong
    # five-expression arbiter whose claim then failed 42P10).
    arbiter = await conn.fetchval(
        "SELECT pg_get_indexdef(($1 || '.wf_step_ledger_claim_uniq')::regclass)", f'"{schema}"'
    )
    if arbiter is None or _normalized_indexdef(arbiter) != _normalized_indexdef(
        _LEDGER_ARBITER_INDEXDEF.replace("{schema}", f'"{schema}"')
    ):
        raise _fail(
            "wf_step_ledger_claim_uniq is not the exact five-expression claim_epoch arbiter"
        )

    # 3. wf_emit_capacity's typed columns, NOT NULL flags, PRIMARY KEY,
    #    and its own two CHECK constraints — each CHECK validated by
    #    NAME, contype, and EXACT normalized body (D4's equality law):
    #    a COUNT of CHECK constraints accepted a same-name CHECK (true)
    #    pair — or a UNIQUE constraint reusing a dropped constraint's
    #    name — whose loss then admitted bound=0 and
    #    wake_generation < handled_generation rows on real PostgreSQL.
    cols = await conn.fetch(
        """SELECT attname, format_type(atttypid, atttypmod) AS t, attnotnull
           FROM pg_attribute
           WHERE attrelid = ($1 || '.wf_emit_capacity')::regclass
             AND NOT attisdropped AND attnum > 0""",
        f'"{schema}"',
    )
    col_types = {r["attname"]: (r["t"], r["attnotnull"]) for r in cols}
    for name, want in (
        ("flow_id", "uuid"),
        ("bound", "integer"),
        ("wake_generation", "bigint"),
        ("handled_generation", "bigint"),
    ):
        got = col_types.get(name)
        if got is None or got[0] != want or not got[1]:
            raise _fail(f"wf_emit_capacity.{name} is missing, mistyped, or nullable")
    capacity_constraints = {
        r["conname"]: (_contype(r["contype"]), r["def"])
        for r in await conn.fetch(
            """SELECT conname, contype, pg_get_constraintdef(oid) AS def
               FROM pg_constraint
               WHERE conrelid = ($1 || '.wf_emit_capacity')::regclass""",
            f'"{schema}"',
        )
    }
    checks = _missing_check_constraint(capacity_constraints, _CAPACITY_CHECKS, "wf_emit_capacity")
    if checks is not None:
        raise _fail(checks)
    # The capacity row's own identity invariant: the flow_id PRIMARY KEY
    # is the §2.1 at-most-one-intent-per-flow law's enforcement.
    pk = _missing_primary_key(capacity_constraints, ("flow_id",), "wf_emit_capacity")
    if pk is not None:
        raise _fail(pk)

    # 4. the rotation singleton's WHOLE shape — its typed columns, its
    #    id PRIMARY KEY, and its own CHECK (id), each validated with the
    #    same name+body rigour (a same-name CHECK (true) would admit a
    #    second id=false row beside the singleton) — then the singleton
    #    row itself.
    rotation_cols = await conn.fetch(
        """SELECT attname, format_type(atttypid, atttypmod) AS t, attnotnull
           FROM pg_attribute
           WHERE attrelid = ($1 || '.wf_emit_intent_rotation')::regclass
             AND NOT attisdropped AND attnum > 0""",
        f'"{schema}"',
    )
    rotation_types = {r["attname"]: (r["t"], r["attnotnull"]) for r in rotation_cols}
    for name, want in (("id", "boolean"), ("after_flow_id", "uuid")):
        got = rotation_types.get(name)
        if got is None or got[0] != want or not got[1]:
            raise _fail(f"wf_emit_intent_rotation.{name} is missing, mistyped, or nullable")
    rotation_constraints = {
        r["conname"]: (_contype(r["contype"]), r["def"])
        for r in await conn.fetch(
            """SELECT conname, contype, pg_get_constraintdef(oid) AS def
               FROM pg_constraint
               WHERE conrelid = ($1 || '.wf_emit_intent_rotation')::regclass""",
            f'"{schema}"',
        )
    }
    checks = _missing_check_constraint(
        rotation_constraints, _ROTATION_CHECKS, "wf_emit_intent_rotation"
    )
    if checks is not None:
        raise _fail(checks)
    pk = _missing_primary_key(rotation_constraints, ("id",), "wf_emit_intent_rotation")
    if pk is not None:
        raise _fail(pk)
    rotation = await conn.fetchval(
        "SELECT after_flow_id FROM " + f'"{schema}".wf_emit_intent_rotation WHERE id'
    )
    if rotation is None:
        raise _fail("wf_emit_intent_rotation's singleton row is missing")

    # 5. the pending-generation index (EXACT definition equality — D4).
    pending = await conn.fetchval(
        "SELECT pg_get_indexdef(($1 || '.wf_emit_capacity_pending_idx')::regclass)",
        f'"{schema}"',
    )
    if pending is None or _normalized_indexdef(pending) != _normalized_indexdef(
        _PENDING_IDX_INDEXDEF.replace("{schema}", f'"{schema}"')
    ):
        raise _fail("wf_emit_capacity_pending_idx is not the exact pending-generation index")

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

    # 7. the exact-park partial index (EXACT definition equality via
    # pg_get_indexdef — D4: the substring check accepted a same-name
    # wrong index whose probe then planned a Seq Scan on jobs). Read
    # pg_type_is_visible in the same query so the comparison matches
    # PostgreSQL's own search_path-dependent enum cast qualification
    # without mutating the caller's session or transaction search_path.
    park_row = await conn.fetchrow(
        """SELECT pg_get_indexdef(to_regclass($1 || '.jobs_wf_emit_park_idx')) AS indexdef,
                  pg_type_is_visible(to_regtype($1 || '.job_status')) AS status_visible""",
        f'"{schema}"',
    )
    park_idx = park_row["indexdef"] if park_row is not None else None
    status_visible = park_row["status_visible"] if park_row is not None else None
    if (
        park_idx is None
        or status_visible is None
        or _normalized_indexdef(park_idx)
        != _normalized_indexdef(
            _expected_park_indexdef(schema, status_type_visible=bool(status_visible))
        )
    ):
        raise _fail("jobs_wf_emit_park_idx is not the exact-park partial index definition")

    _EMIT_OBJECTS_OK.add(schema)
