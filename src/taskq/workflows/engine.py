"""The workflow engine (T04): the two-tx finalize, the guarded decrement +
exactly-once fire, the lock-first sweep, the outbox drain, the fork.

THE JOIN-WAIT REPRESENTATION — defined EXACTLY ONCE, here (T03/T08/T15
reference this name and no other): a joined node waits as
``status='pending'`` + ``deps_pending > 0`` (+ ``metadata.blocking_reason
='join'``). NO new ENUM value: ``pending`` is legal in the vanilla machine
(``VALID_TRANSITIONS`` unchanged), and the dispatch claim's
``AND deps_pending = 0`` exclusion (T03, benched) keeps join-wait rows
unclaimable. When the counter hits 0 the row becomes claimable by the claim
predicate — no status write.

FINALIZE = TWO TRANSACTIONS (P1 FINAL, verbatim):
* **tx1** — the result write, carrying the TERMINAL-MARK FENCE
  (``status='running' AND locked_by_worker=… AND attempt=… AND
  claim_epoch=…`` — the attempt is the fencing token, hardening H8). A row
  the reclaim flipped, or a cancelled flow, updates nothing. The fork's
  child INSERTs + edge rows + join node share tx1 (FORK ATOMICITY: the
  adversarial crash matrix heals via reclaim + re-fork — exactly N children
  + 1 join, never partial, never double). The step ledger's terminal write
  rides tx1 (the ledger-terminal-atomic rule, hardening H9), and the
  failure IO-capture rides tx1 (a row already being updated; the success
  path writes nothing extra).
  **tx2 runs ONLY when tx1's fenced UPDATE returned a row** — the ROWCOUNT
  GATE; a fenced tx1 updates nothing, so tx2 never executes (this is what
  makes 50 duplicate finalizes → 1 decrement).
* **tx2** — the guarded decrement: ONE atomic
  ``UPDATE jobs SET deps_pending = deps_pending - 1 WHERE deps_pending > 0
  … RETURNING`` over the edge ledger, with the flow-status EXISTS leg
  inside the statement; rows hitting 0 → ``INSERT INTO wf_join_fire … ON
  CONFLICT DO NOTHING RETURNING`` → only the guard winner (a row actually
  returned) runs the reducer body (INSIDE tx2 — the exactly-once boundary
  is the body's boundary; a raising reducer rolls tx2 back and the body
  RE-RUNS on re-fire: at-least-once body execution) and writes the outbox
  row. The ``wf_join_fire`` UNIQUE prevents double FIRES; the rowcount gate
  prevents PREMATURE ones.

THE SWEEP = LOCK-FIRST RE-DERIVE: ``FOR UPDATE SKIP LOCKED`` the join-wait
children FIRST, then count un-terminal parents from the edge ledger inside
the same transaction. In-flight decrements are skipped this pass (the
healthy worker wins); committed decrements are visible to the count. Stale
writes impossible by construction — there is no snapshot-derived counter
write. SET-BASED, NOT N+1: ONE batched statement (the per-join round-trip
variant measured p50 477 ms / p95 1.73 s @ ~340 live joins vs the set-based
14.9 ms @ 200); the shipped arm cites both numbers and pins the set-based
plan.

THE EIGHTH RULE: derived values are written only by the statement that
derives them. No carried snapshots; status transitions CAS-only; the
decrement owned exclusively by the decrement statement of tx2, guarded
``deps_pending > 0``.

COUNTER-AS-CACHE / LEDGER-AS-TRUTH: the join counter's truth is THE EDGE
LEDGER — ``remaining = join_target - committed decrements`` — NEVER
child-row presence. Nested joins are exactly where the children don't exist
yet (child-row counting reads 0 and fires early — the EMPTY-JOIN dragon);
the edge-ledger formula is load-bearing FOR THE NESTED SHAPE.

FORK ATOMICITY: the parent's guarded terminal UPDATE + the fork's child
INSERTs + the join-row INSERT share ONE transaction (one ``now()``). The
idempotent fork-debt reconcile is a REPAIR for the legacy/degraded case —
the design is the atomic fork; the split-tx variant (a TERMINAL PARENT WITH
A FORK DEBT) is the red, kept forever.

P3's rules this engine ships: rule 1's held-row inertness invariant (a held
row — ``scheduled_at`` in the future, the signal deadline — is invisible to
every arm; the budget arm itself is T19's); rule 2's ledger discipline (the
attempt increments at claim, the only grant of work); rule 4's THREE cancel
legs (the fire guard's flow-status leg + the dispatch fence in the claim +
the finalize fence); rule 5 (decrement + fire = one transaction, tx2); rule
6 (the finalize CAS IS the shape guard — no read-based second guard); rule
7 (ladder retries emit NO terminal — the decrement happens only at
exhaustion).

All ids this engine mints go through ``taskq._ids`` (uuid7, time-ordered) —
never ``gen_random_uuid()``, never ``uuid4`` (TID251; the seam-only
generation pin greps this package). Deadlock retry (hardening H7): the
finalize side retries on a real ``DeadlockDetectedError`` — the aborted TX
had no effect, so the retry is linearization-preserving.

THE POOL-ZOMBIE RULE (the fanout proof's cut #7): after
``pg_terminate_backend``, the worker's connection is released before
asyncpg processes the death — the next claim grabs a zombie. On a
crash-death the caller must ``await conn.close()`` before re-raising so the
pool discards it; the crash-recovery fixtures (T15) exercise it.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final, Literal

import asyncpg
import structlog

from taskq._ids import new_uuid
from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import ConnLike, JobId
from taskq.backend.statemachine import assert_valid_transition
from taskq.workflows._capture import build_capture
from taskq.workflows._sql import (
    BLOCKING_REASON_JOIN,
    BLOCKING_REASON_ORPHAN_PARENT,
    WorkflowSql,
)

__all__ = [
    "DISPATCH_EXCLUSION_CLAUSE",
    "ChildSpec",
    "ConsumerBinding",
    "ConsumerDefaults",
    "DeadlockRetriesExhaustedError",
    "DecrementHit",
    "FinalizeResult",
    "FiredJoin",
    "ForkSpec",
    "JoinSpec",
    "NodeSpec",
    "SweepResult",
    "drain_outbox",
    "finalize_node",
    "insert_node",
    "reap_phantom_ledger",
    "render_workflow_sql",
    "sweep_join_rederive",
]

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)

#: The dispatch exclusion clause — the claim statement's WHERE gains this
#: (T03 owns the clause and its bench). Named once, here; the dispatch SQL
#: in ``backend/_dispatch_sql.py`` spells it, and this constant documents
#: the engine-side contract.
DISPATCH_EXCLUSION_CLAUSE: Final[str] = "deps_pending = 0"

#: Deadlock-retry budget (hardening H7): BOTH operators (this finalize side
#: and T10's cancel side) retry — the aborted TX had no effect, so the retry
#: preserves linearization. Jittered backoff between attempts.
_DEADLOCK_RETRIES: Final[int] = 4
_DEADLOCK_BACKOFF_BASE_S: Final[float] = 0.05

#: Fork fan-out chunk size: the fan-out inserts children in chunks of this
#: many rows per parallel-array statement (a chunked INSERT inside the SAME
#: tx — never a second transaction, which would break fork atomicity).
_FORK_CHUNK: Final[int] = 500


@dataclass(frozen=True, slots=True)
class ConsumerBinding:
    """A fired join's downstream consumer (dispatched as a normal step by
    the outbox drain — no hand-wired glue, no out-of-engine decode)."""

    step_key: str
    actor: str
    queue: str
    payload: Any = None
    map_index: int | None = None


@dataclass(frozen=True, slots=True)
class ChildSpec:
    """One fan-out child of a forking node. Ids are minted per fork by the
    engine (uuid7 via the seam) — the wiring lives in ``parent_id`` +
    ``map_index``, NEVER in a string-shape convention on hand-built ids."""

    step_key: str
    actor: str
    queue: str
    payload: Any = None
    map_index: int | None = None


@dataclass(frozen=True, slots=True)
class JoinSpec:
    """The join node a fork creates: born in join-wait with the declared
    parent count (its fan-out's children). Its consumers ride the outbox."""

    step_key: str
    actor: str
    queue: str
    payload: Any = None
    consumers: tuple[ConsumerBinding, ...] = ()


@dataclass(frozen=True, slots=True)
class ForkSpec:
    """The fork a node's success-finalize performs, ATOMIC with its
    terminal mark (one tx: the mark, the children, the edges, the join)."""

    children: tuple[ChildSpec, ...]
    join: JoinSpec | None = None
    trace_id: str | None = None
    max_attempts: int = 3
    retry_kind: str = "transient"


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """One workflow node's enqueue (the workflow-row INSERT's spec)."""

    flow_id: JobId
    step_key: str
    actor: str
    queue: str
    payload: Any = None
    parent_id: JobId | None = None
    map_index: int | None = None
    deps_pending: int = 0
    trace_id: str | None = None
    max_attempts: int = 3
    retry_kind: str = "transient"
    idempotency_scope: str = ""
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class DecrementHit:
    join_job_id: JobId
    deps_pending: int


@dataclass(frozen=True, slots=True)
class FiredJoin:
    join_job_id: JobId
    step_key: str


@dataclass(frozen=True, slots=True)
class FinalizeResult:
    applied: bool
    attempt: int | None
    decremented: tuple[DecrementHit, ...] = ()
    fired: tuple[FiredJoin, ...] = ()


@dataclass(frozen=True, slots=True)
class SweepResult:
    blocked: int
    reconciled: int
    firable: int
    fired: tuple[FiredJoin, ...] = ()


@dataclass(frozen=True, slots=True)
class ConsumerDefaults:
    """The drain's consumer-row defaults (the bindings carry the payload;
    actor/queue resolution is T09's compile step — the core's default is the
    flow's own placement until then)."""

    actor: str = "workflow"
    queue: str = "default"
    max_attempts: int = 3
    retry_kind: str = "transient"


class DeadlockRetriesExhaustedError(RuntimeError):
    """The deadlock-retry budget exhausted (hardening H7's loud failure)."""


def render_workflow_sql(schema: str) -> WorkflowSql:
    """The statement bundle for one validated schema (the engine's SQL
    entry — every statement is a named constant, never an inline f-string)."""
    return WorkflowSql.build(schema)


def _jsonb(value: Any) -> str | None:
    """Encode a jsonb bind value (dict → str; None passes through)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return dumps_jsonb_str(value)


def _metadata(flow_id: JobId, *, blocking_reason: str | None) -> dict[str, Any]:
    meta: dict[str, Any] = {"flow_id": str(flow_id)}
    if blocking_reason is not None:
        meta["blocking_reason"] = blocking_reason
    return meta


def _join_metadata(flow_id: JobId) -> dict[str, Any]:
    return _metadata(flow_id, blocking_reason=BLOCKING_REASON_JOIN)


# ── Enqueue: the workflow-node insert ───────────────────────────────────


async def insert_node(conn: ConnLike, wsql: WorkflowSql, spec: NodeSpec) -> JobId:
    """Insert one workflow node row.

    A JOINED node (``deps_pending > 0``) is born in join-wait: metadata
    carries ``blocking_reason='join'``. Vanilla enqueues never set
    ``deps_pending`` (DEFAULT 0 — a semantic no-op for them). The parent
    COLUMN is the only parent truth (never derived from a node-id string);
    ``trace_id`` is stamped on the same insert (§18.2).
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
        _join_metadata(spec.flow_id)
        if spec.deps_pending > 0
        else _metadata(spec.flow_id, blocking_reason=None),
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


def _jsonb_len(value: dict[str, Any] | None) -> int | None:
    if value is None:
        return None
    return len(_jsonb(value).encode("utf-8"))


async def _run_tx1(
    conn: asyncpg.Connection,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    job_id: JobId,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int | None,
    outcome: str,
    result: dict[str, Any] | None,
    error_class: str | None,
    error_message: str | None,
    error_traceback: str | None,
    fork: ForkSpec | None,
    capture: dict[str, Any] | None,
    step_key: str,
) -> bool:
    """tx1 in ONE transaction: the fenced terminal mark; the ledger's
    terminal write; the fork's children + edges + join node. Returns
    whether the fence admitted the write (the rowcount gate tx2 keys on).
    A fenced write records the attempt 'fenced' on the ledger (H1) — never
    a running row left forever on a terminal flow."""
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
            await conn.execute(wsql.ledger_fence_attempt, flow_id, step_key, attempt, error_class)
            return False

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
        )

        if fork is not None:
            await _insert_fork(conn, wsql, flow_id=flow_id, parent_id=job_id, fork=fork)
    return True


async def _insert_fork(
    conn: asyncpg.Connection,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    parent_id: JobId,
    fork: ForkSpec,
) -> tuple[list[JobId], JobId | None]:
    """The fork's writes, inside the CALLER's transaction (tx1): the child
    rows, the edge rows, the join node — parallel-array chunks, ids minted
    app-side (uuid7 via the seam). NEVER a second transaction: the parent's
    terminal mark, the children, and the join share one tx, one now()."""
    children = fork.children
    child_ids = [new_uuid() for _ in children]

    for start in range(0, len(children), _FORK_CHUNK):
        chunk = children[start : start + _FORK_CHUNK]
        ids = child_ids[start : start + _FORK_CHUNK]
        await conn.execute(
            wsql.fork_children,
            ids,
            [c.actor for c in chunk],
            [c.queue for c in chunk],
            [_jsonb(c.payload) for c in chunk],
            [c.map_index for c in chunk],
            [fork.max_attempts] * len(chunk),
            [fork.retry_kind] * len(chunk),
            [parent_id] * len(chunk),
            [c.map_index for c in chunk],
            [c.step_key for c in chunk],
            [fork.trace_id] * len(chunk),
            [_metadata(flow_id, blocking_reason=None)] * len(chunk),
            [f"workflow:{flow_id}"] * len(chunk),
            [
                f"wf:{flow_id}:{c.step_key}"
                + (f":{c.map_index}" if c.map_index is not None else "")
                for c in chunk
            ],
        )
        await conn.execute(
            wsql.fork_edges,
            ids,
            [parent_id] * len(chunk),
            [flow_id] * len(chunk),
        )

    join_id: JobId | None = None
    if fork.join is not None:
        join_id = new_uuid()
        await conn.execute(
            wsql.fork_join_node,
            join_id,
            fork.join.actor,
            fork.join.queue,
            _jsonb(fork.join.payload),
            fork.max_attempts,
            fork.retry_kind,
            parent_id,
            fork.join.step_key,
            len(children),
            fork.trace_id,
            _join_metadata(flow_id),
            f"workflow:{flow_id}",
            f"wf:{flow_id}:{fork.join.step_key}",
        )
        # The join's edges: one per child (the ledger's truth — the
        # fan-out's children feed the join).
        for start in range(0, len(child_ids), _FORK_CHUNK):
            chunk_ids = child_ids[start : start + _FORK_CHUNK]
            await conn.execute(
                wsql.fork_edges,
                # child_id = the join; parent_id = each child.
                [join_id] * len(chunk_ids),
                chunk_ids,
                [flow_id] * len(chunk_ids),
            )
    return child_ids, join_id


async def _run_tx2(
    conn: asyncpg.Connection,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    parent_id: JobId,
    fired_by: str,
    reducers: dict[str, Callable[[], Awaitable[None]]] | None,
) -> tuple[tuple[DecrementHit, ...], tuple[FiredJoin, ...]]:
    """tx2 in ONE transaction: the guarded decrement (the edge ledger, the
    flow-status leg, ``deps_pending > 0``); rows hitting 0 → the guarded
    fire (ON CONFLICT DO NOTHING RETURNING — only the guard winner
    proceeds); the winner's reducer body runs INSIDE tx2; the outbox row
    rides the same tx."""
    async with conn.transaction():
        dec_rows = await conn.fetch(wsql.decrement, parent_id, flow_id)
        hits = tuple(DecrementHit(JobId(r["id"]), r["deps_pending"]) for r in dec_rows)

        fired: list[FiredJoin] = []
        for hit in hits:
            if hit.deps_pending != 0:
                continue
            winner = await conn.fetchrow(
                wsql.fire,
                hit.join_job_id,
                flow_id,
                new_uuid(),
                fired_by,
            )
            if winner is None:
                # The guard refused (another writer fired first, or the
                # flow died): no outbox row, no body — the PK/legs did
                # their job.
                continue
            fired.append(FiredJoin(hit.join_job_id, winner["step_key"]))
            if reducers is not None:
                body = reducers.get(winner["step_key"])
                if body is not None:
                    # The exactly-once boundary is the FIRE's boundary,
                    # never the body's: a raising reducer rolls tx2 back
                    # (the decrement, the fire row and the outbox row all
                    # roll back), the re-derivation re-fires, and the body
                    # RE-RUNS — at-least-once body execution, stated.
                    await body()
            await conn.execute(
                wsql.outbox_insert,
                new_uuid(),
                hit.join_job_id,
                flow_id,
                winner["step_key"],
                None,
                _jsonb({}),
            )
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
    result: dict[str, Any] | None = None,
    error_class: str | None = None,
    error_message: str | None = None,
    error_traceback: str | None = None,
    fork: ForkSpec | None = None,
    reducers: dict[str, Callable[[], Awaitable[None]]] | None = None,
    capture_policy: str = "errors-only",
    capture_max_bytes: int = 8 * 1024,
    redact: Callable[[str], str] | None = None,
    node_input: str | None = None,
) -> FinalizeResult:
    """Finalize one workflow node: tx1 then (rowcount-gated) tx2.

    The attempt-fence: *attempt* is the fencing token — the terminal-mark
    CAS requires ``attempt = $n`` (and the worker + claim-epoch fences), so
    a zombie's write from an abandoned attempt loses to the reclaim +
    re-claim's fresh attempt (hardening H8). A fenced finalize records the
    attempt 'fenced' on the ledger (H1) and reports ``applied=False``.
    """
    assert_valid_transition("running", outcome, job_id)

    # The failure IO-capture rides tx1 (a row already being updated); the
    # success path writes NOTHING extra. Captured fields pass the redact
    # CHAIN first, then the user hook, THEN truncate (chain → hook →
    # truncate; masks are irreversible, so the persisted row never carries
    # a canary — pin 11).
    capture: dict[str, Any] | None = None
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
                fired_by="finalize",
                reducers=reducers,
            )

    hits, fired = await _deadlock_retry(_tx2)
    return FinalizeResult(applied=True, attempt=attempt, decremented=hits, fired=fired)


# ── The sweep: lock-first re-derive + the fire arm ──────────────────────


async def sweep_join_rederive(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    batch_size: int = 200,
    orphan_blocking_reason: str = BLOCKING_REASON_ORPHAN_PARENT,
) -> SweepResult:
    """The lock-first re-derive arm: ONE batched statement (lock the
    join-wait children SKIP LOCKED → count un-terminal parents from the
    edge ledger → reconcile the cache → block the orphan-parent rows →
    report counts), then the set-based fire arm for the firable set (the
    flow-status leg rides INSIDE the fire statement — a post-cancel
    re-derive refuses; the unfenced variant is pin 5's red, kept forever).
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            summary = await conn.fetchrow(wsql.rederive_sweep, batch_size, orphan_blocking_reason)
            assert summary is not None  # the statement always returns its summary row
            firable = summary["firable"]
            fired: tuple[FiredJoin, ...] = ()
            if firable:
                # Ids minted app-side (uuid7 via the seam); the fire
                # statement re-derives the firable set under the row locks
                # this transaction still holds — deterministic — and refuses
                # rows beyond the minted pool (they stay firable next pass).
                fire_ids = [new_uuid() for _ in range(firable)]
                winners = await conn.fetch(wsql.sweep_fire, fire_ids, batch_size)
                fired = tuple(FiredJoin(JobId(w["join_job_id"]), w["step_key"]) for w in winners)
                if fired:
                    await conn.executemany(
                        wsql.outbox_insert,
                        [
                            (
                                new_uuid(),
                                w["join_job_id"],
                                w["flow_id"],
                                w["step_key"],
                                None,
                                "{}",
                            )
                            for w in winners
                        ],
                    )
    return SweepResult(
        blocked=summary["blocked"],
        reconciled=summary["reconciled"],
        firable=firable,
        fired=fired,
    )


# ── The outbox drain ────────────────────────────────────────────────────


async def drain_outbox(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    batch_size: int = 200,
    consumer_defaults: ConsumerDefaults | None = None,
) -> int:
    """The delivery half of the exactly-once fire (re-red-team F6).

    Read undelivered outbox rows (lock-first), insert the consumer rows
    IDEMPOTENTLY on the consumer step key (the composite
    ``(idempotency_scope, idempotency_key)`` arbiter — ``ON CONFLICT DO
    NOTHING``), flip the undelivered flag IN THE INSERT'S TRANSACTION. A
    crash between fire-commit and consumer-insert re-drains: the drain
    completes the dispatch EXACTLY ONCE (pin 20).
    """
    defaults = consumer_defaults or ConsumerDefaults()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(wsql.outbox_fetch_undelivered, batch_size)
            if not rows:
                return 0
            ids = [new_uuid() for _ in rows]
            await conn.execute(
                wsql.outbox_drain_consumers,
                ids,
                [defaults.actor] * len(rows),
                [defaults.queue] * len(rows),
                [r["bindings"] for r in rows],
                [r["map_index"] for r in rows],
                [defaults.max_attempts] * len(rows),
                [defaults.retry_kind] * len(rows),
                [r["join_job_id"] for r in rows],
                [r["map_index"] for r in rows],
                [r["consumer_step_key"] for r in rows],
                [None] * len(rows),
                [_join_metadata(JobId(row["flow_id"])) for row in rows],
                [f"workflow:{r['flow_id']}" for r in rows],
                [
                    f"wf:{r['flow_id']}:{r['consumer_step_key']}"
                    + (f":{r['map_index']}" if r["map_index"] is not None else "")
                    for r in rows
                ],
            )
            await conn.execute(wsql.outbox_drain_flip, [r["id"] for r in rows])
    return len(rows)


# ── The phantom reaper (hardening H1-H3's sweep arm) ────────────────────


async def reap_phantom_ledger(pool: asyncpg.Pool, wsql: WorkflowSql) -> int:
    """Fence every 'running' ledger row whose FLOW is terminal — the
    rows-alone reconstruction reconciles (pin 15). Returns the reaped count."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(wsql.phantom_reap)
    return len(rows)
