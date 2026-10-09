"""The step ledger + the run-key arbiter + the idempotency contract (T05).

Idempotency keys are assigned as ``scope = workflow run``,
``key = (step_key [, map_index])`` against the EXISTING composite
``(idempotency_scope, idempotency_key)`` arbiter
(``jobs_idempotency_scope_key_uniq``, ``backend/_enqueue.py``'s speculative
lock budgets and typed conflict path) — NOT the dropped single-column index
(``jobs_idempotency_key_uniq``, dropped by
``01.00.03_01_post_idempotency_scope_drop_old_index.sql``). Idempotency keys
stay TEXT — they are business keys ``(workflow, step_key[, map_index])``,
not surrogate ids; the ledger's surrogate ids ride the seam
(``taskq._ids.new_uuid()``, uuid7 — never DB-side or random-UUID generation (the TID251 ban)).

Per-step opt-out (``idempotent=False``) for steps whose redelivery is
harmless or whose payloads are too large to key. Default ON — silent
double-runs are the failure class this layer exists to prevent.

Map-child retries claim the same row: the retried child's step key is
``(workflow, map node, map_index)`` → the ON CONFLICT path returns the
recorded result rather than re-executing (the memoized lookup consults any
prior TERMINAL ledger row for the step; ``attempt`` is NOT in that key — a
retried child claims a NEW attempt row, but the replay returns the recorded
result).

RUN-LEVEL IDEMPOTENCY (G2): ``workflows.run(flow, input, key=…)`` claims
against the SAME composite arbiter with ``scope = 'workflow-run'``: a
conflict RETURNS THE EXISTING RUN (its id + status), never a second silent
run. The pin's origin is the founding incident (a stale fixed key silently
returned a PRIOR run's id with a 202 and launched nothing — "the dedup
lives in whoever remembers the key"): the run key makes the ARBITER the
rememberer.

CRON x WORKFLOW COMPOSITION (G3 — the rule's ONE home): a cron entry fires
``wf.run`` with the CRON-SLOT KEY as the run key (G2's arbiter —
``scope='workflow-run', key='<flow>:<slot-timestamp>'``), so the two dedup
regimes COMPOSE instead of racing: same slot twice → ONE run.

THE HONEST BOUNDARY (the contract the docs state as semantics): exactly-once
for DB-LOCAL effects (steps share the enqueue connection pattern; the ledger
PK physically blocks double-recording); AT-LEAST-ONCE for external effects,
with the ledger dedup. Join/reducer BODIES are covered by the same boundary:
a raising reducer rolls tx2 back and the body RE-RUNS on re-fire.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ConnLike, JobId
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.workflows._sql import WorkflowSql

if TYPE_CHECKING:
    # Type-only export: JSONValue lives in tors' stubs, not its runtime
    # module -- future annotations make the string-form uses safe.
    from tors import JSONValue

__all__ = [
    "FlowEntry",
    "LedgerClaim",
    "RunClaim",
    "RunClaimKind",
    "claim_step_ledger",
    "insert_flow_run",
    "memoized_step_result",
    "run_idempotency_scope",
    "step_idempotency_key",
    "step_idempotency_scope",
]

#: The run-key arbiter's scope (G2). The step-level scope is
#: ``workflow:{flow_id}`` (one flow run namespaces its own step keys).
#: THE SCOPE IS NAMESPACED PER FLOW (the run-key collision attack's cure):
#: :func:`run_idempotency_scope` composes ``workflow-run:<flow name>`` —
#: the bare constant is the PRE-FIX shape, a GLOBAL scope in which two
#: DIFFERENT workflows sharing a naive key (a slot timestamp, ``"nightly"``)
#: collide silently (the second flow's run returned the FIRST flow's run
#: id + status, launched nothing).
RUN_IDEMPOTENCY_SCOPE: Final[str] = "workflow-run"

#: The typed claim verdicts (the claim surface's honest vocabulary).
RunClaimKind = Literal["created", "existing-running", "existing-terminal"]

#: The terminal set the ``existing-terminal`` verdict reads (the
#: state machine's own vocabulary — never a re-spelled literal).
TERMINAL_JOB_STATUSES: Final[frozenset[str]] = frozenset(TERMINAL_STATUSES)


class FlowEntry(Protocol):
    """The flow definition's registered shape the run-key claim needs (the
    typed door — a bare ``int`` or attribute-less object is a checker
    error, the T01 negative probes pin it). ``name`` namespaces the
    arbiter's scope (``workflow-run:<name>``)."""

    name: str
    actor: str
    queue: str
    max_attempts: int
    retry_kind: str
    payload: dict[str, object] | str | None
    trace_id: str | None


def run_idempotency_scope(flow_name: str | None = None) -> str:
    """The run-key arbiter's scope: ``workflow-run:<flow name>`` when the
    flow definition is named (the shipped shape — one flow's keys never
    dedup another flow's run), the bare prefix when anonymous."""
    if not flow_name:
        return RUN_IDEMPOTENCY_SCOPE
    return f"{RUN_IDEMPOTENCY_SCOPE}:{flow_name}"


def step_idempotency_scope(flow_id: JobId) -> str:
    """The step-claim arbiter's scope: one flow run."""
    return f"workflow:{flow_id}"


def step_idempotency_key(step_key: str, map_index: int | None = None) -> str:
    """The step-claim arbiter's key: ``(step_key[, map_index])``."""
    return f"wf:{step_key}" if map_index is None else f"wf:{step_key}:{map_index}"


def _decode_jsonb(value: Any) -> Any:
    """asyncpg returns jsonb as ``str`` on un-coded connections -- the
    ledger's contract is the DECODED value; parse before returning (the
    estate's _json seam, never the stdlib import)."""
    if isinstance(value, str):
        return _json_loads(value)
    return value


@dataclass(frozen=True, slots=True)
class LedgerClaim:
    """The claim's outcome (the ON CONFLICT path's one-round-trip RETURNING).

    ``fresh`` rows were inserted by THIS claim (the only grant of work — the
    attempt increments at claim); a conflicting claim returns the EXISTING
    row, whose terminal ``result`` is the memoized answer.
    ``result`` is the TYPED door: the memoized payload is a JSON value —
    never a bare ``Any`` (the T01 probe: any method call on it is a checker
    error).
    ``ledger_id`` is the claimed LEDGER ROW's own id (the RETURNING id) —
    the strongest terminal-write key: the finalize that holds it pins the
    outcome write to exactly this row (``LEDGER_TERMINAL_BY_ID_SQL``).
    """

    flow_id: JobId
    job_id: JobId
    step_key: str
    map_index: int | None
    attempt: int
    status: str
    result: JSONValue | None
    error_class: str | None
    error_message: str | None
    ledger_id: JobId | None = None


@dataclass(frozen=True, slots=True)
class RunClaim:
    """The run-key arbiter's outcome: ``created`` rows are THIS caller's new
    run; a conflicting caller gets the EXISTING run's id + status — never a
    second silent run.

    THE CLAIM SURFACE HONEST (the run-key failure lie's cure): the
    pre-cure surfaces discarded ``status`` and returned a bare id — the
    caller could not tell 'already running' from 'already failed' without
    a second query, and a FAILED run's key replayed as a silent 202. The
    typed verdict is :attr:`kind`: ``created`` (THIS caller's run),
    ``existing-running`` (the live run's id — idempotent replay), or
    ``existing-terminal`` (the REFUSED-TO-REUSE verdict, stated LOUDLY:
    a terminal run's key is never silently re-fired — the re-run is the
    caller's documented choice, a NEW key, and the claim carries the
    prior run's id + status so the caller can act on it).
    """

    flow_id: JobId
    created: bool
    status: str

    @property
    def kind(self) -> RunClaimKind:
        """The typed verdict (the T01 door's law: a member access, never a
        bare value to decode at the call site)."""
        if self.created:
            return "created"
        if self.status in TERMINAL_JOB_STATUSES:
            return "existing-terminal"
        return "existing-running"


async def claim_step_ledger(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    job_id: JobId,
    step_key: str,
    map_index: int | None,
    attempt: int,
) -> LedgerClaim:
    """The step-ledger claim: ONE round trip (P1 FINAL's idempotent-claim
    shape — ``INSERT … ON CONFLICT DO UPDATE … RETURNING``, never
    check-then-insert; verified 30 reps x 10 concurrent).

    THE LEDGER-CLAIM-ATOMIC RULE (hardening H6): the claim and the attempt's
    ``running`` ledger row are ONE statement — a two-transaction variant
    leaves a cancel in the window fencing NOTHING (the attempt isn't on the
    record yet), the row appears POST-CANCEL as a phantom ``running``.
    """
    row_id = new_uuid()
    rec = await conn.fetchrow(
        wsql.ledger_claim,
        row_id,
        flow_id,
        job_id,
        step_key,
        map_index,
        attempt,
    )
    assert rec is not None  # ON CONFLICT DO UPDATE always returns the row
    return LedgerClaim(
        ledger_id=JobId(rec["id"]),
        flow_id=JobId(rec["flow_id"]),
        job_id=JobId(rec["job_id"]),
        step_key=rec["step_key"],
        map_index=rec["map_index"],
        attempt=rec["attempt"],
        status=rec["status"],
        result=_decode_jsonb(rec["result"]),
        error_class=rec["error_class"],
        error_message=rec["error_message"],
    )


async def memoized_step_result(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    step_key: str,
    map_index: int | None,
) -> LedgerClaim | None:
    """The latest TERMINAL ledger row for ``(flow, step[, map_index])`` —
    the map-child retry's ``ON CONFLICT`` path: the recorded result returns
    rather than re-executing. ``None`` when no terminal row exists."""
    rec = await conn.fetchrow(wsql.ledger_memoized, flow_id, step_key, map_index)
    if rec is None:
        return None
    return LedgerClaim(
        flow_id=flow_id,
        job_id=JobId(rec["job_id"]),
        step_key=step_key,
        map_index=map_index,
        attempt=rec["attempt"],
        status=rec["status"],
        result=_decode_jsonb(rec["result"]),
        error_class=rec["error_class"],
        error_message=rec["error_message"],
    )


async def insert_flow_run(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    entry: FlowEntry,
    run_key: str,
) -> RunClaim:
    """The RUN-KEY claim (G2): the flow's root row inserted under the
    ``workflow-run:<flow name>`` scope with the caller's key — the composite
    arbiter is the rememberer. A conflict returns the EXISTING run's id +
    status.

    *entry* is the flow definition's registered shape (T09's API; the
    TYPED door — :class:`FlowEntry`, the protocol the negative probes
    pin).
    """
    flow_id = new_uuid()
    # THE WORKFLOW NAME STAMP (the reducer resolution's durable leg): the
    # root's metadata names the workflow whose REGISTERED DEFINITION
    # carries the run's step bodies — the sweep's fire arm resolves a
    # healed join's reducer from that definition (D1, BODY-FROM-
    # DEFINITION), in whatever process heals. Schema-level, not
    # process-level: the memo in workflows/_reducers.py is a cache only.
    root_metadata: dict[str, object] = {"flow_id": str(flow_id)}
    if entry.name:
        root_metadata["workflow"] = entry.name
    inserted = await conn.fetchrow(
        wsql.flow_run_insert,
        flow_id,
        entry.actor,
        entry.queue,
        entry.payload if isinstance(entry.payload, str) else dumps_jsonb_str(entry.payload or {}),
        entry.max_attempts,
        entry.retry_kind,
        entry.trace_id,
        dumps_jsonb_str(root_metadata),
        run_idempotency_scope(entry.name),
        run_key,
    )
    if inserted is not None:
        return RunClaim(flow_id=JobId(inserted["id"]), created=True, status=inserted["status"])

    # The arbiter's conflict path: the EXISTING run's id + status, never a
    # second silent run (the founding-incident shape — "202 + a new run" —
    # is the convicted variant, kept RED forever by the pin).
    existing = await conn.fetchrow(wsql.flow_run_read, run_idempotency_scope(entry.name), run_key)
    assert existing is not None  # the arbiter conflict implies the row exists
    return RunClaim(
        flow_id=JobId(existing["id"]),
        created=False,
        status=existing["status"],
    )
