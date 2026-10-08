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

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import asyncpg
import structlog

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.obs import get_logger

__all__ = [
    "HOLD_CHANNEL",
    "DeliveryResult",
    "HitlClient",
    "delivery_refused",
    "register_hold",
    "sweep_expired_signals",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The notify channel (the knock — the estate's pg_notify discipline;
#: the row is the truth, a missed message converges by polling).
HOLD_CHANNEL = "taskq_wf_holds"

#: The hold row's status vocabulary (the statemachine's totality for the
#: signal rows).
SignalStatus = Literal["held", "delivered", "cancelled", "abandoned"]


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
    is_loop_node: bool,
) -> JobId:
    """Register ONE hold: a NEW epoch mints a NEW row (multi-hold — the
    same signal name can hold again; the stale-payload dragon dies
    here); the node takes the HELD REPRESENTATION (T03: ``pending`` +
    ``scheduled_at`` = deadline + the signal row as truth — the worker
    releases, no slot held); a LOOP node's budget PAUSES (the wall is
    blind while the loop waits on a human)."""
    hold_id = JobId(new_uuid())
    deadline = await _deadline_expr(conn, timeout_s)
    async with conn.transaction():
        await conn.execute(
            _render(_HOLD_INSERT_SQL, schema),
            hold_id,
            workflow_id,
            node_key,
            signal_name,
            hold_epoch,
            call_id,
            dumps_jsonb_str({}),  # the payload arrives at DELIVER (deliver-no-drop)
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


async def deliver_payload(
    pool: asyncpg.Pool,
    *,
    schema: str,
    workflow_id: JobId,
    hold_id: JobId,
    payload: dict[str, object],
    payload_json: str,
) -> DeliveryResult:
    """THE DELIVER CAS: validate (the caller's typed door did) → the
    row's ``'held' → 'delivered'`` CAS → the node's resume. Exactly-once:
    two concurrent delivers → ONE ``delivered``, one resume. A deliver
    that cannot resume = the TYPED ``refused`` (the hold SURVIVES on the
    stale arm) — never a silent drop."""
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
        validates through the bound gate's door at the CALLER's type
        level; the CAS makes the double-resolve the DEFINED no-op (an
        already-resolved hold = one transition, never two — the
        idempotence pin). THE AUDIT (G4): the resolve is the
        audit-sensitive action par excellence — "who resolved this" is a
        ROW, not a log line (the admin's `_audit` module, lazily
        imported — the layering law)."""
        async with self._pool.acquire() as conn, conn.transaction():
            # THE GUARD (read-hold — never a pre-set transition): the
            # DELIVER CAS owns the 'held' → 'delivered' move (a pre-set
            # here would make the deliver's own CAS lose to it — the
            # double-transition bug, convicted); the guard + the audit +
            # the knock share THIS tx, the deliver's CAS is the
            # linearization point.
            guarded = await conn.fetchrow(
                _render(
                    "SELECT workflow_id, node_key, signal_name FROM {schema}.wf_signals "
                    "WHERE id = $1 AND status = 'held' FOR UPDATE",
                    self._schema,
                ),
                hold_id,
            )
            if guarded is None:
                already = await conn.fetchval(
                    _render("SELECT status FROM {schema}.wf_signals WHERE id = $1", self._schema),
                    hold_id,
                )
                return DeliveryResult(
                    status="no-op" if already == "delivered" else "refused",
                    reason=f"hold {hold_id} is {already!r}",
                )
            # THE AUDIT ROW (the caller owns the tx — the same-tx
            # guarantee).
            from taskq.web.admin._audit import record_admin_action

            await record_admin_action(
                conn,
                schema=self._schema,
                principal=principal,
                action="hitl.resolve",
                target_type="hold",
                target_id=str(hold_id),
                reason=reason,
                detail={"run_id": str(guarded["workflow_id"]), "signal": guarded["signal_name"]},
            )
            # THE KNOB (the same tx — the pointer, never the truth).
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                HOLD_CHANNEL,
                dumps_jsonb_str(
                    {
                        "hold_id": str(hold_id),
                        "run_id": str(guarded["workflow_id"]),
                        "event": "resolved",
                    }
                ),
            )
        workflow_id = JobId(
            UUID(str(guarded["workflow_id"]))
        )  # the uuid column → JobId (the Record's member walks through the str form's round-trip)
        return await deliver_payload(
            self._pool,
            schema=self._schema,
            workflow_id=workflow_id,
            hold_id=JobId(hold_id if isinstance(hold_id, UUID) else UUID(str(hold_id))),
            payload=decision,
            payload_json=dumps_jsonb_str(decision),
        )

    def _context(self, row: Any) -> HoldContext:
        """The row → the context (decoded ONCE — cut #14's law; the
        REDACT LAW: the payload passes the chain before anything
        leaves)."""
        payload = _json_loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        schema_ref = (
            _json_loads(row["payload_schema"])
            if isinstance(row["payload_schema"], str)
            else row["payload_schema"]
        )
        if self._redact is not None and payload is not None:
            payload = self._redact(payload)
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


async def cancel_run_signals(pool: asyncpg.Pool, *, schema: str, workflow_id: JobId) -> int:
    """The CANCEL CASCADE's signal leg: the run's held signals →
    ``cancelled`` (the same one-tx cancel — the caller owns the
    transaction's flow flip; a LATE operator deliver returns the typed
    ``refused`` — no zombie wake, no resume event, pin 1's shape)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(_render(_CANCEL_RUN_SIGNALS_SQL, schema), workflow_id)
    return len(rows)
