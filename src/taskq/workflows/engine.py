"""The workflow engine's finalize mechanics (T04): the two-tx finalize,
the dispatch exclusion, the deadlock retry.

THE JOIN-WAIT REPRESENTATION — defined EXACTLY ONCE, here (T03/T08/T15
reference this name and no other): a joined node waits as
``status='pending'`` + ``deps_pending > 0`` (+ ``metadata.blocking_reason
='join'``). NO new ENUM value: ``pending`` is legal in the vanilla machine
(``VALID_TRANSITIONS`` unchanged), and the dispatch claim's
``AND deps_pending = 0`` exclusion (T03, benched) keeps join-wait rows
unclaimable. When the counter hits 0 the row becomes claimable by the
claim predicate — no status write.

FINALIZE = TWO TRANSACTIONS (P1 FINAL, verbatim):
* **tx1** — the result write, carrying the TERMINAL-MARK FENCE
  (``status='running' AND locked_by_worker=… AND attempt=… AND
  claim_epoch=…`` — the attempt is the fencing token, hardening H8). A row
  the reclaim flipped, or a cancelled flow, updates nothing. The fork's
  child INSERTs + edge rows + join node share tx1 (FORK ATOMICITY —
  :mod:`taskq.workflows._fork`: a kill at any statement boundary rolls the
  whole tx back; the reclaim re-forks — exactly N children + 1 join, never
  partial, never double). The step ledger's terminal write rides tx1 (the
  ledger-terminal-atomic rule, hardening H9), and the failure IO-capture
  rides tx1 (a row already being updated; the success path writes NOTHING
  extra).
  **tx2 runs ONLY when tx1's fenced UPDATE returned a row** — the ROWCOUNT
  GATE; a fenced tx1 updates nothing, so tx2 never executes (this is what
  makes 50 duplicate finalizes → 1 decrement).
* **tx2** — the guarded decrement: ONE atomic
  ``UPDATE jobs SET deps_pending = deps_pending - 1 WHERE deps_pending > 0
  … RETURNING`` over the edge ledger, with the flow-status EXISTS leg
  inside the statement; rows hitting 0 → the guarded fire
  (``INSERT INTO wf_join_fire … ON CONFLICT DO NOTHING RETURNING``) → only
  the guard winner (a row actually returned) runs the reducer body (INSIDE
  tx2 — the exactly-once boundary is the body's boundary; a raising
  reducer rolls tx2 back and the body RE-RUNS on re-fire: at-least-once
  body execution) and writes the outbox rows (one per declared consumer).
  The ``wf_join_fire`` UNIQUE prevents double FIRES; the rowcount gate
  prevents PREMATURE ones.

THE EIGHTH RULE: derived values are written only by the statement that
derives them. No carried snapshots; status transitions CAS-only; the
decrement owned exclusively by the decrement statement of tx2, guarded
``deps_pending > 0``.

COUNTER-AS-CACHE / LEDGER-AS-TRUTH: the join counter's truth is THE EDGE
LEDGER — ``remaining = join_target - committed decrements`` — NEVER
child-row presence. Nested joins are exactly where the children don't
exist yet (child-row counting reads 0 and fires early — the EMPTY-JOIN
dragon); the edge-ledger formula is load-bearing FOR THE NESTED SHAPE.

A PROMOTED NESTED MAP decrements its grandparent's join in the same
transaction: the nested map's reduce node finalizes through the SAME tx2
decrement — its edges point at the grandparent's join. Nested maps, hence
sequencing, hence a depth-N DAG, stay expressible.

P3's rules this engine ships: rule 1's held-row inertness invariant (a
held row — ``scheduled_at`` in the future, the signal deadline — is
invisible to every arm; the budget arm itself is T19's); rule 2's ledger
discipline (the attempt increments at claim, the only grant of work);
rule 4's THREE cancel legs (the fire guard's flow-status leg + the
dispatch fence in the claim + the finalize fence); rule 5 (decrement +
fire = one transaction, tx2); rule 6 (the finalize CAS IS the shape guard
— no read-based second guard); rule 7 (ladder retries emit NO terminal —
the decrement happens only at exhaustion).

All ids this engine mints go through ``taskq._ids`` (uuid7, time-ordered)
— never DB-side or random-UUID generation (the TID251 ban; the seam-only
generation pin greps this package).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import Any, Final, Literal

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, JobId
from taskq.backend.statemachine import assert_valid_transition
from taskq.workflows._capture import build_capture
from taskq.workflows._fork import insert_fork
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import (
    DecrementHit,
    FinalizeResult,
    FiredJoin,
    ForkSpec,
    NodeSpec,
    _consumer_bindings,
    _join_metadata,
    _jsonb,
    _metadata,
)

__all__ = [
    "DISPATCH_EXCLUSION_CLAUSE",
    "DeadlockRetriesExhaustedError",
    "FinalizeResult",
    "finalize_node",
    "insert_node",
    "render_workflow_sql",
]

#: The dispatch exclusion clause — the claim statement's WHERE gains this
#: (T03 owns the clause and its bench). Named once, here; the dispatch SQL
#: in ``backend/_dispatch_sql.py`` spells it, and this constant documents
#: the engine-side contract.
DISPATCH_EXCLUSION_CLAUSE: Final[str] = "deps_pending = 0"

#: Deadlock-retry budget (hardening H7): BOTH operators (this finalize side
#: and T10's cancel side) retry — the aborted TX had no effect, so the
#: retry preserves linearization. Jittered backoff between attempts.
_DEADLOCK_RETRIES: Final[int] = 4
_DEADLOCK_BACKOFF_BASE_S: Final[float] = 0.05


class DeadlockRetriesExhaustedError(RuntimeError):
    """The deadlock-retry budget exhausted (hardening H7's loud failure)."""


def render_workflow_sql(schema: str) -> WorkflowSql:
    """The statement bundle for one validated schema (the engine's SQL
    entry — every statement is a named constant, never an inline f-string)."""
    return WorkflowSql.build(schema)


# ── Enqueue: the workflow-node insert ───────────────────────────────────


async def insert_node(conn: ConnLike, wsql: WorkflowSql, spec: NodeSpec) -> JobId:
    """Insert one workflow node row.

    A JOINED node (``deps_pending > 0``) is born in join-wait: metadata
    carries ``blocking_reason='join'`` (+ the declared consumers). Vanilla
    enqueues never set ``deps_pending`` (DEFAULT 0 — a semantic no-op for
    them). The parent COLUMN is the only parent truth (never derived from a
    node-id string); ``trace_id`` is stamped on the same insert (§18.2).
    """
    if spec.deps_pending < 0:
        raise ValueError(f"deps_pending must be >= 0, got {spec.deps_pending}")
    node_id = JobId(new_uuid())
    await conn.execute(
        wsql.node_insert,
        node_id,
        spec.actor,
        spec.queue,
        _jsonb(spec.payload),
        spec.max_attempts,
        spec.retry_kind,
        spec.parent_id,
        spec.map_index,
        spec.step_key,
        spec.deps_pending,
        spec.trace_id,
        _jsonb(_join_metadata(spec.flow_id, spec.consumers))
        if spec.deps_pending > 0
        else _jsonb(_metadata(spec.flow_id, blocking_reason=None)),
        spec.idempotency_scope,
        spec.idempotency_key,
    )
    return node_id


# ── The deadlock retry (hardening H7) ───────────────────────────────────


async def _deadlock_retry(coro_factory: Callable[[], Awaitable[Any]]) -> Any:
    """Run *coro_factory* retrying on a real Postgres deadlock (H7).

    Each attempt builds a fresh coroutine (the aborted TX had no effect;
    the retry is linearization-preserving). Backoff: jittered exponential —
    S311-scoped (timing jitter, not crypto).
    """
    for attempt in range(_DEADLOCK_RETRIES + 1):
        try:
            return await coro_factory()
        except asyncpg.exceptions.DeadlockDetectedError:
            if attempt == _DEADLOCK_RETRIES:
                raise DeadlockRetriesExhaustedError(
                    f"deadlock retry budget exhausted after {_DEADLOCK_RETRIES + 1} attempts"
                ) from None
            await asyncio.sleep(_DEADLOCK_BACKOFF_BASE_S * (2**attempt) * (0.5 + random.random()))


# ── Finalize: tx1 + tx2 ─────────────────────────────────────────────────


def _jsonb_len(value: dict[str, object] | None) -> int | None:
    if value is None:
        return None
    return len(_jsonb(value).encode("utf-8"))


async def _run_tx1(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    job_id: JobId,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int | None,
    outcome: str,
    result: dict[str, object] | None,
    error_class: str | None,
    error_message: str | None,
    error_traceback: str | None,
    fork: ForkSpec | None,
    capture: dict[str, object] | None,
    step_key: str,
    map_index: int | None = None,
    ledger_id: JobId | None = None,
) -> bool:
    """tx1 in ONE transaction: the fenced terminal mark; the ledger's
    terminal write; the fork's children + edges + join node. Returns
    whether the fence admitted the write (the rowcount gate tx2 keys on).
    A fenced write records the attempt 'fenced' on the ledger (H1) — never
    a running row left forever on a terminal flow.

    THE LEDGER TERMINAL'S KEY: the claim's OWN RETURNING id when the caller
    holds it (*ledger_id* — ``LEDGER_TERMINAL_BY_ID_SQL``, the write pins
    exactly the claimed row), otherwise the full arbiter tuple with the
    COALESCE(map_index, -1) expression (``LEDGER_TERMINAL_SQL``) — a map
    child's terminal can never overwrite its sibling's row (the ledger-PK
    attack's overwrite arm)."""
    async with conn.transaction():
        rec = await conn.fetchrow(
            wsql.terminal_mark,
            job_id,
            outcome,
            _jsonb(result),
            _jsonb_len(result),
            error_class,
            error_message,
            error_traceback,
            worker_id,
            attempt,
            claim_epoch,
        )
        if rec is None:
            # Fenced out (a reclaimed row, a cancelled flow, a wrong
            # worker): the ledger says 'fenced', tx2 never runs.
            if ledger_id is not None:
                await conn.execute(wsql.ledger_fence_by_id, ledger_id, error_class)
            else:
                await conn.execute(
                    wsql.ledger_fence_attempt,
                    flow_id,
                    step_key,
                    attempt,
                    error_class,
                    map_index,
                )
            return False

        if ledger_id is not None:
            await conn.execute(
                wsql.ledger_terminal_by_id,
                ledger_id,
                outcome,
                _jsonb(result),
                error_class,
                error_message,
                _jsonb(capture),
            )
        else:
            await conn.execute(
                wsql.ledger_terminal,
                flow_id,
                step_key,
                attempt,
                outcome,
                _jsonb(result),
                error_class,
                error_message,
                _jsonb(capture),
                map_index,
            )

        if fork is not None:
            await insert_fork(
                conn,
                wsql,
                flow_id=flow_id,
                parent_id=job_id,
                parent_step_key=step_key,
                fork=fork,
            )
    return True


async def _fire_and_deliver(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    join_id: JobId,
    fire_id: JobId,
    fired_by: str,
    reducers: dict[str, Callable[[], Awaitable[None]]] | None,
) -> FiredJoin | None:
    """One guarded fire + its delivery: the fire statement (the rowcount
    gate, the join-wait status, the flow-status EXISTS leg, the UNIQUE's
    exactly-once) → the winner → the reducer body INSIDE the caller's tx2
    → one outbox row per declared consumer. ``None`` = the guard refused
    (another writer fired first, or the flow died): no outbox row, no body
    — the PK/legs did their job."""
    winner = await conn.fetchrow(wsql.fire, join_id, flow_id, fire_id, fired_by)
    if winner is None:
        return None
    fired = FiredJoin(
        join_job_id=JobId(winner["join_job_id"]),
        step_key=winner["step_key"],
        consumers=_consumer_bindings(winner["consumers"]),
    )
    if reducers is not None:
        body = reducers.get(winner["step_key"])
        if body is not None:
            # The exactly-once boundary is the FIRE's boundary, never the
            # body's: a raising reducer rolls tx2 back (the decrement, the
            # fire row and the outbox rows all roll back), the re-derivation
            # re-fires, and the body RE-RUNS — at-least-once body execution,
            # stated.
            await body()
    if fired.consumers:
        await conn.executemany(
            wsql.outbox_insert,
            [
                (
                    new_uuid(),
                    fired.join_job_id,
                    flow_id,
                    c.step_key,
                    c.map_index,
                    _jsonb({"actor": c.actor, "queue": c.queue, "payload": c.payload}),
                )
                for c in fired.consumers
            ],
        )
    return fired


async def _run_tx2(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    parent_id: JobId,
    reducers: dict[str, Callable[[], Awaitable[None]]] | None,
) -> tuple[tuple[DecrementHit, ...], tuple[FiredJoin, ...]]:
    """tx2 in ONE transaction: the guarded decrement (the edge ledger, the
    flow-status leg, ``deps_pending > 0``); rows hitting 0 → the guarded
    fire; the winner's reducer body runs INSIDE tx2; the outbox rows ride
    the same tx."""
    async with conn.transaction():
        dec_rows = await conn.fetch(wsql.decrement, parent_id, flow_id)
        hits = tuple(DecrementHit(JobId(r["id"]), r["deps_pending"]) for r in dec_rows)

        fired: list[FiredJoin] = []
        for hit in hits:
            if hit.deps_pending != 0:
                continue
            result = await _fire_and_deliver(
                conn,
                wsql,
                flow_id=flow_id,
                join_id=hit.join_job_id,
                fire_id=JobId(new_uuid()),
                fired_by="finalize",
                reducers=reducers,
            )
            if result is not None:
                fired.append(result)
    return hits, tuple(fired)


async def finalize_node(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    job_id: JobId,
    step_key: str,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int | None,
    outcome: Literal["succeeded", "failed", "cancelled", "crashed", "abandoned"],
    result: dict[str, object] | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
    error_traceback: str | None = None,
    fork: ForkSpec | None = None,
    reducers: dict[str, Callable[[], Awaitable[None]]] | None = None,
    capture_policy: str = "errors-only",
    capture_max_bytes: int = 8 * 1024,
    redact: Callable[[str], str] | None = None,
    node_input: str | None = None,
    map_index: int | None = None,
    ledger_id: JobId | None = None,
) -> FinalizeResult:
    """Finalize one workflow node: tx1 then (rowcount-gated) tx2.

    The attempt-fence: *attempt* is the fencing token — the terminal-mark
    CAS requires ``attempt = $n`` (and the worker + claim-epoch fences), so
    a zombie's write from an abandoned attempt loses to the reclaim +
    re-claim's fresh attempt (hardening H8). A fenced finalize records the
    attempt 'fenced' on the ledger (H1) and reports ``applied=False``.

    The LEDGER TERMINAL's key: *ledger_id* (the claim's own RETURNING id —
    ``LedgerClaim.ledger_id``) when held, else the arbiter tuple keyed with
    *map_index* — either way the write touches exactly the claimed row, so
    two map children of one step key never overwrite each other's outcome.
    """
    assert_valid_transition("running", outcome, job_id)

    # The failure IO-capture rides tx1 (a row already being updated); the
    # success path writes NOTHING extra (the G1 ruling — every capture
    # policy's success path is inert; "all" is reserved by the same rule).
    # Captured fields pass the redact CHAIN first, then the user hook, THEN
    # truncate (chain → hook → truncate; masks are irreversible, so the
    # persisted row never carries a canary — pin 11).
    capture: dict[str, object] | None = None
    if outcome == "failed":
        capture = build_capture(
            policy=capture_policy,
            node_input=node_input,
            error=error_message,
            max_bytes=capture_max_bytes,
            redact=redact,
        )

    async def _tx1() -> bool:
        async with pool.acquire() as conn:
            return await _run_tx1(
                conn,
                wsql,
                flow_id=flow_id,
                job_id=job_id,
                worker_id=worker_id,
                attempt=attempt,
                claim_epoch=claim_epoch,
                outcome=outcome,
                result=result,
                error_class=error_class,
                error_message=error_message,
                error_traceback=error_traceback,
                fork=fork,
                capture=capture,
                step_key=step_key,
                map_index=map_index,
                ledger_id=ledger_id,
            )

    applied = await _deadlock_retry(_tx1)
    if not applied:
        return FinalizeResult(applied=False, attempt=None)

    async def _tx2() -> tuple[tuple[DecrementHit, ...], tuple[FiredJoin, ...]]:
        async with pool.acquire() as conn:
            return await _run_tx2(
                conn,
                wsql,
                flow_id=flow_id,
                parent_id=job_id,
                reducers=reducers,
            )

    hits, fired = await _deadlock_retry(_tx2)
    return FinalizeResult(applied=True, attempt=attempt, decremented=hits, fired=fired)
