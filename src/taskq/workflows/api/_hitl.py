"""The HITL runtime (T10): the hold rows, the deliver CAS, the expiry
arm, the client surface — HUMAN-IN-THE-LOOP AS ROWS.

THE ROW IS THE TRUTH; THE KNOB IS A POINTER: the hold's appearance is
NOTIFIABLE (the estate's pg_notify — the same channel discipline as the
cancel wake), but the notification carries THE POINTER only (hold id +
run id + gate) — NEVER the sole copy of anything: a lost notification
costs latency, never correctness (the consumer converges by polling
``client.hitl.list()`` — the SSE seq-cursor doctrine).

THE DELIVER CAS (the fence): validate → the row's
``'held' → 'delivered'`` CAS → the node's held-representation cleared →
the resume. Send twice = no-op: two concurrent delivers → exactly ONE
``delivered`` result, exactly one resume. A deliver that validates but
cannot resume (a cancelled flow, a stale epoch) = the TYPED
``DeliveryResult(status="refused", ...)`` the deliverer SEES — never a
silent drop (the proven adversarial shape: the smuggled
``Approval.model_construct(verdict=42)`` is refused at the boundary with
the named error, the hold SURVIVES, the worker never crashes).

RESUME-NOT-RETRY (A-CRITICAL-3, cut #5): a hold's resume consumes NO
ladder attempts — the ledger distinguishes ``awaited`` from ``failed``;
the ladder counts ``failed`` only (the proven matrix:
``awaited → failed → failed`` ENTERS the ladder; ``awaited →
succeeded`` never touches it). The shared-counter variant — the claim
incrementing the ladder on every claim, so 2 holds + max_attempts=3 =
terminal failure with ZERO retries — is pin 7's RED forever.

THE REDACT LAW EXTENDS TO THE CONTEXT (T04's composition verdict —
chain then hook): the hold's context passes the SAME redact chain
BEFORE it reaches any enumeration/pubsub surface — a canary secret in
tool args must no more reach the client list than the capture row.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import UUID

import asyncpg
import structlog
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.obs import get_logger
from taskq.workflows._capture import redact_hold_context

__all__ = [
    "HOLD_CHANNEL",
    "HOLD_CREATED_CHANNEL",
    "HOLD_EXPIRED_CHANNEL",
    "HOLD_RESOLVED_CHANNEL",
    "DeliveryResult",
    "HitlClient",
    "delivery_refused",
    "register_hold",
    "register_signal_models",
    "resolve_payload_fit",
    "resolve_signal_models",
    "sweep_expired_signals",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The notify channel (the knock — the estate's pg_notify discipline;
#: the row is the truth, a missed message converges by polling).
HOLD_CHANNEL = "taskq_wf_holds"

#: THE BROADCAST CHANNELS (T26 — global names, the SCHEMA RIDES THE
#: PAYLOAD: pg_notify is per-database and the schema-per-module estate
#: shares one database across many schemas, so per-schema channel names
#: would buy nothing; the listener filters by the payload's schema).
#: These are the TYPED legs (the listener's events decode them); the
#: legacy :data:`HOLD_CHANNEL` knob above keeps its pinned pointer shape.
HOLD_CREATED_CHANNEL = "taskq_wf_hold"
HOLD_RESOLVED_CHANNEL = "taskq_wf_hold_resolved"
HOLD_EXPIRED_CHANNEL = "taskq_wf_hold_expired"

#: The hold row's status vocabulary (the statemachine's totality for the
#: signal rows).
SignalStatus = Literal["held", "delivered", "cancelled", "abandoned"]


# ── THE SIGNAL MODEL CATALOG (the typed door's runtime registry) ────────
# The deliver path's boundary validates the payload against the WAIT
# SITE's declared models (attack-3 B2/H2's cure) — but the wait site is
# body code: the classes exist only where the body ran. The catalog is
# the same D1 discipline the reducer memo uses: the process that ran the
# wait site registers the models (every process in the fleet carries the
# same definitions, so a resolve in THIS process validates); a cold
# process falls back to the hold row's own ``payload_schema`` (the
# models' JSON schemas — durable, read by :func:`_fits_by_schema`).

_SignalModels = tuple[
    tuple[type[BaseModel], ...], Callable[[dict[str, object]], type[BaseModel]] | None
]

_signal_models: dict[tuple[str, str, str], _SignalModels] = {}


def register_signal_models(
    workflow_name: str,
    node_key: str,
    signal_name: str,
    models: tuple[type[BaseModel], ...],
    discriminator: Callable[[dict[str, object]], type[BaseModel]] | None = None,
) -> None:
    """The wait site's declared models enter the catalog (keyed by the
    workflow + node + signal identity — the hold row's own coordinates).
    Idempotent: a re-run of the same wait site re-registers the SAME
    models (the re-execution doctrine)."""
    _signal_models[(workflow_name, node_key, signal_name)] = (models, discriminator)


def resolve_signal_models(
    workflow_name: str | None,
    node_key: str,
    signal_name: str,
) -> _SignalModels | None:
    """The declared models for one hold's coordinates — ``None`` when
    this process never ran the wait site (the caller falls back to the
    row's ``payload_schema``)."""
    if not workflow_name:
        return None
    return _signal_models.get((workflow_name, node_key, signal_name))


def _model_fits(model: type[BaseModel], payload: dict[str, object]) -> bool:
    """Does *payload* fit ONE declared model, STRICTLY (by SHAPE, never
    by declaration order — attack-3 B2's rule): no unknown-field
    coercion (a key the model does not declare fails the fit — the
    all-optional ``Lenient`` door cannot swallow a strict member's
    delivery), every required field present and validatable."""
    allowed: set[str] = set()
    for name, field_info in model.model_fields.items():
        allowed.add(name)
        if field_info.alias is not None:
            allowed.add(field_info.alias)
    if not set(payload) <= allowed:
        return False
    try:
        model.model_validate(payload)
    except Exception:
        return False
    return True


def _fits_by_schema(payload: object, schema: object) -> bool:
    """The COLD-PROCESS fallback fit: the payload against ONE model's
    JSON schema (the durable ``payload_schema``). A conservative
    structural check — required fields present, unknown fields refused,
    primitive types checked; a schema the check cannot read (nested
    ``$defs``, ``anyOf``) FITS (never a false refusal — the class-loaded
    path validates fully)."""
    if not isinstance(schema, dict) or not isinstance(payload, dict):
        return True  # cannot judge — refuse nothing on a guess
    if "$defs" in schema or "anyOf" in schema or "allOf" in schema:
        return True
    properties = schema.get("properties")  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]  # Why: the jsonb walk's boundary — the schema is the row's decoded jsonb; the isinstance guard below is the runtime shape check.
    if not isinstance(properties, dict):
        return True
    # The Any-contract walk (the estate's house shape): every branch
    # asserts the runtime shape it consumes; each USE of an Unknown
    # member carries the targeted ignore with the Why.
    properties_doc = cast("dict[str, object]", properties)
    payload_doc = cast("dict[str, object]", payload)
    if any(key not in properties_doc for key in payload_doc):
        return False  # STRICT: no unknown-field coercion
    required = schema.get("required", [])  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]  # Why: the same jsonb walk.
    if isinstance(required, list):
        required_doc = cast("list[object]", required)
        if any(key not in payload_doc for key in required_doc):
            return False
    for key, value in payload_doc.items():
        spec = properties_doc.get(key)
        if not isinstance(spec, dict):
            continue
        declared = spec.get("type")  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]  # Why: the same jsonb walk.
        if declared == "string" and not isinstance(value, str):
            return False
        if declared == "integer" and (not isinstance(value, int) or isinstance(value, bool)):
            return False
        if declared == "number" and (isinstance(value, bool) or not isinstance(value, int | float)):
            return False
        if declared == "boolean" and not isinstance(value, bool):
            return False
        if declared == "array" and not isinstance(value, list):
            return False
        if declared == "object" and not isinstance(value, dict):
            return False
    return True


def resolve_payload_fit(
    models: tuple[type[BaseModel], ...],
    payload: object,
    discriminator: Callable[[dict[str, object]], type[BaseModel]] | None = None,
) -> type[BaseModel]:
    """THE BY-SHAPE RESOLUTION (attack-3 B2's cure): the payload is
    validated against EACH declared model (strict —
    :func:`_model_fits`); the models that fit are the candidates.
    EXACTLY ONE must fit — it is returned. ZERO fits → the typed
    :class:`taskq.exceptions.SignalPayloadError`; MORE than one → the
    typed :class:`taskq.exceptions.SignalPayloadAmbiguousError` UNLESS
    the gate declared an explicit *discriminator* (its pick must be one
    of the fitting candidates — a discriminator naming a non-fitting
    model is the same refusal). Declaration ORDER decides nothing."""
    from taskq.exceptions import SignalPayloadAmbiguousError, SignalPayloadError

    if not isinstance(payload, dict):
        raise SignalPayloadError(
            f"the delivered payload is {type(payload).__name__!r}, not an "
            "object — the declared models "
            f"({[m.__name__ for m in models]}) validate objects"
        )
    payload_doc = cast("dict[str, object]", payload)  # pyright: ignore[reportUnknownVariableType]  # Why: the delivered payload is the row's jsonb — the isinstance guard above is the runtime shape check (the Any-contract walk's boundary).
    fitting = [m for m in models if _model_fits(m, payload_doc)]
    if len(fitting) == 1:
        return fitting[0]
    names = [m.__name__ for m in models]
    if not fitting:
        raise SignalPayloadError(
            f"the delivered payload validates against NONE of the wait's "
            f"declared models ({names}) — the delivery is refused, the "
            "hold SURVIVES (the typed door's runtime boundary)"
        )
    if discriminator is not None:
        chosen = discriminator(payload_doc)
        if chosen in fitting:
            return chosen
        raise SignalPayloadAmbiguousError(
            f"the payload fits {len(fitting)} declared models ({names}) "
            f"and the gate's discriminator picked {chosen.__name__!r}, "
            "which is not one of them — the delivery is refused"
        )
    raise SignalPayloadAmbiguousError(
        f"the delivered payload fits {len(fitting)} declared models "
        f"({names}) and the gate declared no discriminator — narrowing by "
        "declaration order is the convicted mis-narrowing; declare a "
        "discriminator on the wait site or deliver an unambiguous shape"
    )


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """The typed delivery verdict the deliverer SEES (deliver-no-drop):
    ``delivered`` (the resume fired), ``no-op`` (already resolved — the
    CAS lost to an earlier deliver: exactly-once), ``refused`` (the
    payload validated but nothing resumable is there — the hold
    SURVIVES on a refused delivery's stale-epoch arm)."""

    status: Literal["delivered", "no-op", "refused"]
    reason: str | None = None


def delivery_refused(reason: str) -> DeliveryResult:
    """The refused verdict's constructor (the named shape)."""
    return DeliveryResult(status="refused", reason=reason)


# ── the statements (named constants — the schema is the only
#    interpolation, validated via require_schema; values all $N-bound) ──

_HOLD_INSERT_SQL = """\
INSERT INTO {schema}.wf_signals
    (id, workflow_id, node_key, signal_name, hold_epoch, call_id,
     payload, payload_schema, status, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, 'held', $9)
RETURNING created_at
"""

_HOLD_HOLD_NODE_SQL = """\
UPDATE {schema}.jobs
SET status = 'pending',
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    metadata = metadata || $2::jsonb,
    scheduled_at = COALESCE($3::timestamptz, scheduled_at),
    budget_paused = CASE WHEN metadata @> '{"kind": "loop"}'::jsonb THEN true ELSE budget_paused END
WHERE id = $1
  AND status = 'running'
RETURNING id
"""

_DELIVER_CAS_SQL = """\
UPDATE {schema}.wf_signals
SET status = 'delivered',
    payload = $3::jsonb,
    resolved_at = clock_timestamp()
WHERE id = $2
  AND workflow_id = $1
  AND status = 'held'
RETURNING node_key, signal_name, call_id
"""

_DELIVER_RESUME_SQL = """\
UPDATE {schema}.jobs
SET metadata = metadata - 'hold' - 'held_signal',
    scheduled_at = now(),
    budget_paused = false
WHERE id = $1
  AND status = 'pending'
RETURNING id
"""

_LIST_SQL = """\
SELECT id, workflow_id, node_key, signal_name, hold_epoch, call_id,
       payload, payload_schema, status, created_at, expires_at
FROM {schema}.wf_signals
WHERE workflow_id = $1
  AND status = 'held'
ORDER BY id
"""

_GET_SQL = """\
SELECT id, workflow_id, node_key, signal_name, hold_epoch, call_id,
       payload, payload_schema, status, created_at, expires_at, resolved_at
FROM {schema}.wf_signals
WHERE id = $1
"""

_HOLD_AWAITED_LEDGER_SQL = """\
UPDATE {schema}.wf_step_ledger
SET status = 'awaited', updated_at = clock_timestamp()
WHERE id = $1
RETURNING id
"""

_CANCEL_RUN_SIGNALS_SQL = """\
UPDATE {schema}.wf_signals
SET status = 'cancelled', resolved_at = clock_timestamp()
WHERE workflow_id = $1
  AND status = 'held'
RETURNING id
"""

_EXPIRY_SWEEP_SQL = """\
WITH expired AS (
    SELECT id, workflow_id, node_key, signal_name, hold_epoch, call_id, expires_at
    FROM {schema}.wf_signals
    WHERE status = 'held'
      AND expires_at IS NOT NULL
      AND expires_at <= clock_timestamp()
    ORDER BY id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE {schema}.wf_signals s
SET status = 'abandoned', resolved_at = clock_timestamp()
FROM expired e
WHERE s.id = e.id
RETURNING s.id, s.workflow_id, s.node_key, s.signal_name, s.expires_at
"""


def _render(template: str, schema: str) -> str:
    from taskq.constants import require_schema

    require_schema(schema)
    return template.replace("{schema}", schema)


async def _broadcast(conn: ConnLike, channel: str, payload: dict[str, object]) -> None:
    """THE BROADCAST LEG (T26): ONE ``pg_notify`` executed on the
    CALLER'S connection, INSIDE the caller's transaction — the leg is
    transactional by construction: PG delivers a NOTIFY only when its
    transaction COMMITS, so a rolled-back hold-create (or a losing
    resolve CAS) is SILENT (pin T26-P1). The payload carries the schema
    (the channels are global — the listener filters by it) and the
    POINTER only, never the sole copy of anything (the row is the
    truth)."""
    await conn.execute("SELECT pg_notify($1, $2)", channel, dumps_jsonb_str(payload))


def _as_hold_id(hold_id: JobId | str) -> JobId:
    """The id's canonical form (the uuid column → JobId; a str handle
    round-trips through the UUID parse)."""
    return hold_id if isinstance(hold_id, UUID) else JobId(UUID(str(hold_id)))


# ── the hold registration (the wait site's door) ────────────────────────


async def mark_awaited(conn: ConnLike, schema: str, ledger_id: JobId) -> None:
    """RESUME-NOT-RETRY's ledger face: the CURRENT attempt's ledger row
    records ``awaited`` — the hold's resume consumes NO ladder attempts
    (the ladder counts ``failed`` only). The claim's OWN row is the
    record (an INSERT here would collide with the claim's arbiter key —
    the UniqueViolation the pins convict)."""
    await conn.execute(_render(_HOLD_AWAITED_LEDGER_SQL, schema), ledger_id)


async def register_hold(
    conn: ConnLike,
    *,
    schema: str,
    workflow_id: JobId,
    node_id: JobId,
    node_key: str,
    signal_name: str,
    hold_epoch: int,
    call_id: str,
    payload_schema: dict[str, object] | None,
    timeout_s: float | None,
    context: dict[str, object] | None = None,
    redact: Callable[[str], str] | None = None,
) -> JobId:
    """Register ONE hold: a NEW epoch mints a NEW row (multi-hold — the
    same signal name can hold again; the stale-payload dragon dies
    here); the node takes the HELD REPRESENTATION (T03: ``pending`` +
    ``scheduled_at`` = deadline + the signal row as truth — the worker
    releases, no slot held); a LOOP node's budget PAUSES (the wall is
    blind while the loop waits on a human).

    THE CONTEXT RIDES THE INSERT'S OWN TRANSACTION (attack-3 H3's cure
    — the crash-window's closure): reason/tool/args pass the redact
    pipeline (chain → the token-head pass → the workflow's hook,
    :func:`redact_hold_context`) and land in the SAME tx as the hold's
    insert — no second statement after the commit, no context-less hold,
    and the row never carries a canary (REDACT-BEFORE-PERSIST extended
    to the hold row).

    THE LOOP'S BUDGET PAUSE IS DERIVED, NEVER ARGUED (the accept-and-
    ignore hunt's cure): register_hold once took ``is_loop_node=`` — the
    caller's belief — and the INSERT ignored it: the pause rode the SQL's
    own marker (``metadata @> '{"kind": "loop"}'``), the row's KIND
    deciding. The kwarg was the accepted-and-ignored class exactly (a
    param consumed by nothing, its promise kept by something else); the
    param is GONE — the row's own kind marker is the only voice, and the
    held-loop pause's pins drill the behavior on the row's kind.

    THE CONTRACT IS MANDATORY (attack-4 F-P4-UNTYPED-COLD-DOOR's cure,
    the mint's half): ``payload_schema`` must carry at least one declared
    model — a contract-less hold is the audit hole (ANY payload delivers
    to it from a fresh process), so the mint REFUSES one. The boundary
    additionally refuses any legacy row that predates the law (the
    migration backfilled those to the explicit no-contract marker)."""
    if not payload_schema:
        raise TypeError(
            "register_hold requires a non-empty payload_schema — a hold "
            "without a declared contract is the audit hole (the typed "
            "door refuses to mint one)"
        )
    hold_id = JobId(new_uuid())
    deadline = await _deadline_expr(conn, timeout_s)
    # THE CONTEXT, REDACTED BEFORE PERSIST: the masked dict is the
    # insert's payload (a mask that broke the JSON shape degrades to the
    # masked TEXT record — the operator still reads a scrubbed row).
    payload_doc: dict[str, object] = {}
    if context is not None and any(value is not None for value in context.values()):
        masked = redact_hold_context(
            {k: v for k, v in context.items() if v is not None}, redact=redact
        )
        try:
            parsed = _json_loads(dumps_jsonb_str(masked))
        except Exception:  # pragma: no cover - dumps just produced the text
            parsed = None
        payload_doc: dict[str, object] = (
            cast("dict[str, object]", parsed)  # pyright: ignore[reportUnknownVariableType]  # Why: the masked context's own jsonb round-trip — the walk's boundary.
            if isinstance(parsed, dict)
            else {"context": dumps_jsonb_str(masked)}
        )
    async with conn.transaction():
        created_at = await conn.fetchval(
            _render(_HOLD_INSERT_SQL, schema),
            hold_id,
            workflow_id,
            node_key,
            signal_name,
            hold_epoch,
            call_id,
            dumps_jsonb_str(
                payload_doc
            ),  # the payload arrives at DELIVER (deliver-no-drop); the CONTEXT is redacted here
            dumps_jsonb_str(payload_schema) if payload_schema else None,
            deadline,
        )
        # THE HELD REPRESENTATION + the held marker (the claimable
        # query's exclusion — the worker RELEASES the slot).
        await conn.execute(
            _render(_HOLD_HOLD_NODE_SQL, schema),
            node_id,
            dumps_jsonb_str({"hold": str(hold_id), "held_signal": signal_name}),
            deadline,
        )
        # THE BROADCAST, CREATE LEG (T26 — TRANSACTIONAL: the NOTIFY
        # rides THIS tx, so a rolled-back hold-create is silent; the
        # payload carries the schema + the pointer, never the payload).
        await _broadcast(
            conn,
            HOLD_CREATED_CHANNEL,
            {
                "schema": schema,
                "flow_id": str(workflow_id),
                "run_id": str(workflow_id),
                "hold_id": str(hold_id),
                "signal": signal_name,
                "node_key": node_key,
                "created_at": created_at.isoformat(),
            },
        )
    return hold_id


async def _deadline_expr(conn: ConnLike, timeout_s: float | None) -> Any:
    """The deadline FROM PG'S CLOCK (the DB-clock doctrine): None → NULL
    (the explicit eternal wait — the W1 warning's subject)."""
    if timeout_s is None:
        return None
    return await conn.fetchval(
        "SELECT now() + ($1::double precision * interval '1 second')", timeout_s
    )


# ── the deliver CAS (the reply door) ────────────────────────────────────

_RESOLVE_CAS_SQL = """\
UPDATE {schema}.wf_signals
SET status = 'delivered',
    payload = $2::jsonb,
    resolved_at = clock_timestamp()
WHERE id = $1
  AND status = 'held'
RETURNING workflow_id, node_key, signal_name, call_id
"""

_HOLD_FOR_BOUNDARY_SQL = """\
SELECT workflow_id, node_key, signal_name, payload_schema, status
FROM {schema}.wf_signals
WHERE id = $1
"""

_WORKFLOW_NAME_SQL = """\
SELECT metadata->>'workflow' FROM {schema}.jobs WHERE id = $1
"""


async def _boundary_verdict(
    pool: asyncpg.Pool,
    *,
    schema: str,
    hold_id: JobId,
    payload: object,
) -> tuple[str | None, str | None]:
    """THE RUNTIME PAYLOAD BOUNDARY (attack-3 B2/H2's cure): the hold's
    DECLARED models validate the payload — BY SHAPE, never by
    declaration order — BEFORE anything consumes the hold. Returns the
    pair (the refusal's REASON — a typed-face string naming WHY — when
    the payload must be refused, ``None`` when it may pass; and the
    FITTED MODEL'S NAME — the verdict's declared kind, the broadcast
    leg's ``verdict_kind``, ``None`` on a refusal or when nothing
    resolved). The models resolve from
    the signal-model catalog (this process ran the wait site — D1's
    discipline); a cold process falls back to the row's own
    ``payload_schema`` (the durable JSON schemas). A hold that is not
    ``'held'`` validates NOTHING here — the CAS owns that refusal (the
    stale arm)."""
    from taskq.exceptions import SignalPayloadAmbiguousError, SignalPayloadError

    async with pool.acquire() as conn:
        row = await conn.fetchrow(_render(_HOLD_FOR_BOUNDARY_SQL, schema), hold_id)
        if row is None or row["status"] != "held":
            return None, None  # the CAS owns the stale/cancelled arm's refusal
        workflow_name = await conn.fetchval(_render(_WORKFLOW_NAME_SQL, schema), row["workflow_id"])
    resolved = resolve_signal_models(workflow_name, row["node_key"], row["signal_name"])
    if resolved is not None:
        models, discriminator = resolved
        try:
            fitted = resolve_payload_fit(models, payload, discriminator)
        except (SignalPayloadError, SignalPayloadAmbiguousError) as exc:
            return str(exc), None
        return None, fitted.__name__
    # THE COLD-PROCESS FALLBACK: the durable payload_schema's structural
    # fit (per declared model, by shape; exactly one must fit).
    schema_ref = row["payload_schema"]
    schema_doc = _json_loads(schema_ref) if isinstance(schema_ref, str) else schema_ref
    if not isinstance(schema_doc, dict) or not schema_doc:
        # THE SCHEMA-LESS HOLD REFUSES (attack-4 F-P4-UNTYPED-COLD-DOOR's
        # cure): a hold with NO declared contract is the audit hole — ANY
        # payload would deliver on THE audit-sensitive action, and the
        # door's teeth would depend on which process asks. There is no
        # honest delivery for an undeclared hold: the LOUD typed refusal,
        # the hold SURVIVES; the operator re-runs the flow on the current
        # code (the wait site mints a declared hold — payload_schema is
        # mandatory at hold time, the migration backfilled the legacy
        # rows to the explicit no-contract marker this arm refuses).
        return (
            "the hold carries NO declared payload schema — the contract the "
            "typed door validates against does not exist on this row (a "
            "legacy/pre-declaration hold). The delivery is REFUSED: nothing "
            "delivers to an undeclared hold. Remedy: cancel the run and "
            "re-run the workflow on the current code, whose wait site "
            "declares the payload contract at hold time",
            None,
        )
    schema_map = cast("dict[str, object]", schema_doc)  # pyright: ignore[reportUnknownVariableType]  # Why: the jsonb walk's boundary — the isinstance guard above is the runtime shape check.
    fitting = [
        name
        for name, js in schema_map.items()  # pyright: ignore[reportUnknownVariableType]  # Why: the same walk.
        if _fits_by_schema(payload, js)
    ]
    if len(fitting) == 1:
        return None, fitting[0]
    if not fitting:
        return (
            f"the delivered payload validates against NONE of the hold's "
            f"declared models ({sorted(schema_map)}) — the delivery is "
            "refused, the hold SURVIVES (the typed door's runtime boundary)",
            None,
        )
    return (
        f"the delivered payload fits {len(fitting)} of the hold's declared "
        f"models ({sorted(schema_map)}) — ambiguous; the delivery is refused",
        None,
    )


async def deliver_payload(
    pool: asyncpg.Pool,
    *,
    schema: str,
    workflow_id: JobId,
    hold_id: JobId,
    payload: dict[str, object],
    payload_json: str,
) -> DeliveryResult:
    """THE DELIVER CAS: validate (the typed door's RUNTIME boundary —
    the payload validates against the hold's declared models BY SHAPE
    before anything consumes the hold; a bad payload is the TYPED
    ``refused`` and the hold SURVIVES) → the row's
    ``'held' → 'delivered'`` CAS → the node's resume. Exactly-once:
    two concurrent delivers → ONE ``delivered``, one resume. A deliver
    that cannot resume = the TYPED ``refused`` (the hold SURVIVES on the
    stale arm) — never a silent drop."""
    refusal, verdict_kind = await _boundary_verdict(
        pool, schema=schema, hold_id=hold_id, payload=payload
    )
    if refusal is not None:
        return delivery_refused(refusal)
    async with pool.acquire() as conn, conn.transaction():
        cas = await conn.fetchrow(
            _render(_DELIVER_CAS_SQL, schema), workflow_id, hold_id, payload_json
        )
        if cas is None:
            already = await conn.fetchval(
                _render("SELECT status FROM {schema}.wf_signals WHERE id = $1", schema),
                hold_id,
            )
            if already == "delivered":
                return DeliveryResult(status="no-op", reason="already delivered")
            return DeliveryResult(
                status="refused",
                reason=f"hold {hold_id} is {already!r} — the deliver CAS refused",
            )
        # THE KNOB (in the same tx): pg_notify — the pointer, never the
        # sole copy (the row is the truth; a missed knock costs latency).
        await conn.execute(
            "SELECT pg_notify($1, $2)",
            HOLD_CHANNEL,
            dumps_jsonb_str(
                {"hold_id": str(hold_id), "run_id": str(workflow_id), "event": "delivered"}
            ),
        )
        # THE BROADCAST, RESOLVED LEG (T26 — the same tx, transactional;
        # verdict_kind is the payload model the typed door validated
        # against — the verdict's declared kind, never the payload).
        await _broadcast(
            conn,
            HOLD_RESOLVED_CHANNEL,
            {
                "schema": schema,
                "hold_id": str(hold_id),
                "flow_id": str(workflow_id),
                "verdict_kind": verdict_kind,
            },
        )
        resumed = None
        if cas["node_key"]:
            node_id = await conn.fetchval(
                _render(
                    "SELECT id FROM {schema}.jobs WHERE step_key = $1 "
                    "AND (metadata->>'flow_id')::uuid = $2 "
                    "AND metadata ? 'hold'",
                    schema,
                ),
                cas["node_key"],
                workflow_id,
            )
            if node_id is not None:
                resumed = await conn.fetchval(_render(_DELIVER_RESUME_SQL, schema), node_id)
        if resumed is None:
            # The CAS won but the node is gone (a cancelled flow killed
            # the held representation): the ROW is the truth — the
            # delivered row stands, the resume is refused.
            return delivery_refused("the held node is not resumable (the flow died)")
        logger.info(
            "signal.delivered",
            run_id=str(workflow_id),
            hold_id=str(hold_id),
            signal=cas["signal_name"],
        )
        return DeliveryResult(status="delivered")


# ── the expiry sweep (the timer arm) ────────────────────────────────────


async def sweep_expired_signals(pool: asyncpg.Pool, *, schema: str, batch_size: int = 100) -> int:
    """The SIGNAL SWEEP's expiry arm (the only live timer on a held row):
    the deadline is DB-CLOCK compared; the expired hold → the DEFINED
    ``abandoned`` state (never a silent orphan) and the node's resume is
    refused-with-type (the body's ``SignalTimeoutError`` — the
    ``on_timeout="fail"`` default's shape; the ``resume_with_default`` /
    ``escalate`` policies' records land with their bodies' reads)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(_render(_EXPIRY_SWEEP_SQL, schema), batch_size)
        for row in rows:
            # The held node's marker clears → the node re-pends; the
            # wait site's resume finds the row 'abandoned' → the typed
            # error (the re-execution doctrine: the body re-runs from
            # the top; pre-wait side effects are ctx.step-ledgered).
            node_id = await conn.fetchval(
                _render(
                    "SELECT id FROM {schema}.jobs WHERE step_key = $1 "
                    "AND (metadata->>'flow_id')::uuid = $2 AND metadata ? 'hold'",
                    schema,
                ),
                row["node_key"],
                row["workflow_id"],
            )
            if node_id is not None:
                await conn.execute(_render(_DELIVER_RESUME_SQL, schema), node_id)
            # THE BROADCAST, EXPIRED LEG (T26): the sweep's CAS UPDATE
            # committed the moment the autocommit statement returned —
            # the notify rides AFTER it, never before (a notify for a
            # row that did not land is the convicted order). The leg is
            # BLIND TO NOTHING: a LOOP-kind hold (budget-paused) expires
            # through this same arm and its event delivers (pin
            # T26-P5's leg).
            await _broadcast(
                conn,
                HOLD_EXPIRED_CHANNEL,
                {
                    "schema": schema,
                    "hold_id": str(row["id"]),
                    "flow_id": str(row["workflow_id"]),
                    "signal": row["signal_name"],
                    "node_key": row["node_key"],
                },
            )
    return len(rows)


# ── the client surface (the programmatic HITL consumer) ─────────────────


@dataclass(frozen=True, slots=True)
class HoldContext:
    """THE HOLD CONTEXT CONTRACT (round-8 requirement 4 — enough for a
    SEPARATE UI to render a useful request): the payload + the
    author-supplied reason/tool/args + the provenance (run id, node
    key, gate name, epoch, created_at, the deadline) + the
    payload-schema reference. THE REDACT LAW EXTENDS: the context passes
    the redact chain BEFORE it reaches this surface (a canary in tool
    args reaches NEITHER the list NOR the knock)."""

    hold_id: str
    run_id: str
    node_key: str
    signal_name: str
    hold_epoch: int
    call_id: str
    payload: Any
    payload_schema: Any
    reason: str | None
    created_at: Any
    expires_at: Any
    status: str


class HitlClient:
    """``client.hitl`` — the programmatic enumeration + resolution (the
    round-8 requirements 1-3): ``list(run=…)`` (ALL pending holds,
    distinctly), ``get(hold_id)`` (the id IS the reply handle),
    ``resolve(hold_id, decision)`` (typed through the bound gate's door;
    idempotent per the statemachine — an already-resolved hold is the
    DEFINED no-op)."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        schema: str,
        redact: Any = None,
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._redact = redact

    async def list(self, run: JobId | str) -> list[HoldContext]:
        """The pending holds for ONE run — a run with MULTIPLE
        concurrent holds lists ALL of them distinctly; the multi-
        workflow property: the filter is BY RUN, resolves never cross."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(_render(_LIST_SQL, self._schema), run)
        return [self._context(r) for r in rows]

    async def get(self, hold_id: JobId | str) -> HoldContext | None:
        """One hold's full context; the id is the reply handle."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(_render(_GET_SQL, self._schema), hold_id)
        return self._context(row) if row is not None else None

    async def resolve(
        self,
        hold_id: JobId | str,
        decision: dict[str, object],
        *,
        reason: str | None = None,
        principal: Any = None,
    ) -> DeliveryResult:
        """THE REPLY (by id — one hold addressed): the decision payload
        validates through the hold's DECLARED models at the RUNTIME
        boundary — BY SHAPE, before the CAS (attack-3 H2's cure): a bad
        payload is the TYPED ``refused``, the hold SURVIVES, nothing is
        consumed, and the refusal is AUDITED (the operator sees WHY).
        The transition + THE AUDIT + THE KNOCK share ONE transaction
        whose linearization point is the DELIVER CAS ITSELF (attack-3
        H4's cure): the loser matches no rows and writes NOTHING — 0
        audit rows, 0 knocks for a lost race (the guard-tx shape wrote
        both BEFORE the CAS and doubled under concurrency). An
        already-resolved hold is the DEFINED no-op (the idempotence
        pin)."""
        from taskq.audit import record_admin_action

        # THE BOUNDARY (before the CAS — nothing consumed on a refusal).
        refusal, verdict_kind = await _boundary_verdict(
            self._pool, schema=self._schema, hold_id=_as_hold_id(hold_id), payload=decision
        )
        if refusal is not None:
            async with self._pool.acquire() as conn, conn.transaction():
                await record_admin_action(
                    conn,
                    schema=self._schema,
                    principal=principal,
                    action="hitl.resolve",
                    target_type="hold",
                    target_id=str(hold_id),
                    reason=reason,
                    detail={"refused": refusal},
                )
            return delivery_refused(refusal)
        # THE RESOLVE TX: the CAS is the transition's only grant; the
        # audit + the knock ride the WINNING tx (exactly-once).
        async with self._pool.acquire() as conn, conn.transaction():
            cas = await conn.fetchrow(
                _render(_RESOLVE_CAS_SQL, self._schema),
                _as_hold_id(hold_id),
                dumps_jsonb_str(decision),
            )
            if cas is None:
                already = await conn.fetchval(
                    _render("SELECT status FROM {schema}.wf_signals WHERE id = $1", self._schema),
                    _as_hold_id(hold_id),
                )
                return DeliveryResult(
                    status="no-op" if already == "delivered" else "refused",
                    reason=f"hold {hold_id} is {already!r}",
                )
            # THE AUDIT ROW (the caller owns the tx — the same-tx
            # guarantee, now exactly-once with the transition).
            await record_admin_action(
                conn,
                schema=self._schema,
                principal=principal,
                action="hitl.resolve",
                target_type="hold",
                target_id=str(hold_id),
                reason=reason,
                detail={"run_id": str(cas["workflow_id"]), "signal": cas["signal_name"]},
            )
            # THE KNOB (the same tx — the pointer, never the truth).
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                HOLD_CHANNEL,
                dumps_jsonb_str(
                    {
                        "hold_id": str(hold_id),
                        "run_id": str(cas["workflow_id"]),
                        "event": "resolved",
                    }
                ),
            )
            # THE BROADCAST, RESOLVED LEG (T26 — the CAS-WINNING tx;
            # the loser matched no rows and knocks NOTHING).
            await _broadcast(
                conn,
                HOLD_RESOLVED_CHANNEL,
                {
                    "schema": self._schema,
                    "hold_id": str(hold_id),
                    "flow_id": str(cas["workflow_id"]),
                    "verdict_kind": verdict_kind,
                },
            )
            resumed = None
            if cas["node_key"]:
                node_id = await conn.fetchval(
                    _render(
                        "SELECT id FROM {schema}.jobs WHERE step_key = $1 "
                        "AND (metadata->>'flow_id')::uuid = $2 "
                        "AND metadata ? 'hold'",
                        self._schema,
                    ),
                    cas["node_key"],
                    cas["workflow_id"],
                )
                if node_id is not None:
                    resumed = await conn.fetchval(
                        _render(_DELIVER_RESUME_SQL, self._schema), node_id
                    )
            if resumed is None:
                # The CAS won but the node is gone (a cancelled flow
                # killed the held representation): the ROW is the truth
                # — the delivered row stands, the resume is refused.
                return delivery_refused("the held node is not resumable (the flow died)")
            logger.info(
                "signal.resolved",
                run_id=str(cas["workflow_id"]),
                hold_id=str(hold_id),
                signal=cas["signal_name"],
            )
            return DeliveryResult(status="delivered")

    def _context(self, row: Any) -> HoldContext:
        """The row → the context (decoded ONCE — cut #14's law; the
        REDACT LAW: the payload passes the chain before anything
        leaves). THE DEFAULT IS THE CHAIN (attack-3 H3's belt): a
        client constructed with no hook still runs the vetted chain +
        the hold-context token pass over the payload — a canary never
        reaches the enumeration surface verbatim even on a row written
        before the persist-time redact existed; a HOOKED client
        composes chain → hook (the hook receives the chain's masked
        payload — it can only redact more)."""
        payload = _json_loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        schema_ref = (
            _json_loads(row["payload_schema"])
            if isinstance(row["payload_schema"], str)
            else row["payload_schema"]
        )
        if payload is not None and isinstance(payload, dict):
            # THE CHAIN RUNS ALWAYS (the vetted masks + the hold
            # surface's token pass); the client's hook POST-COMPOSES on
            # the chain's output — it can only redact more.
            payload_doc = cast("dict[str, object]", payload)  # pyright: ignore[reportUnknownVariableType]  # Why: the row's jsonb — the isinstance guard above is the runtime shape check.
            chained = redact_hold_context(payload_doc)
            payload = chained if self._redact is None else self._redact(chained)
        return HoldContext(
            hold_id=str(row["id"]),
            run_id=str(row["workflow_id"]),
            node_key=row["node_key"],
            signal_name=row["signal_name"],
            hold_epoch=row["hold_epoch"],
            call_id=row["call_id"],
            payload=payload,
            payload_schema=schema_ref,
            reason=None,
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            status=row["status"],
        )


async def cancel_run_signals(conn: ConnLike, *, schema: str, workflow_id: JobId) -> int:
    """The CANCEL CASCADE's signal leg: the run's held signals →
    ``cancelled`` (the same one-tx cancel — THIS leg runs on the CALLER'S
    CONNECTION, inside the caller's transaction: the statement is ONE
    UPDATE, so composing with it costs nothing and the torn state is
    IMPOSSIBLE by construction — the attack-4 finding (F-P4-TORN-CANCEL)
    was the leg riding a SECOND pool connection in autocommit, where an
    outer rollback left ``signal='cancelled'`` standing on a run that
    never cancelled: a held operator's decision destroyed by a cancel
    that never landed. The record cannot lie: the signal write, the root
    flip, and the audit row commit together or not at all)."""
    rows = await conn.fetch(_render(_CANCEL_RUN_SIGNALS_SQL, schema), workflow_id)
    return len(rows)
