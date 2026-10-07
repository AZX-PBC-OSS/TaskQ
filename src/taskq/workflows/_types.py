"""The workflow engine's data types (T04): the specs the enqueue/fork/finalize
paths carry, and the results they return. One home for the wire shapes —
the engine, the fork, and the sweep each import from here, and T09's API
composes them without re-deriving.

Id discipline: every ``*_id: JobId`` is uuid7 via the ``taskq._ids`` seam —
never DB-side or random-UUID generation (the TID251 ban).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from taskq._json import dumps_jsonb_str
from taskq.backend._protocol import JobId
from taskq.workflows._sql import BLOCKING_REASON_JOIN

__all__ = [
    "ChildSpec",
    "ConsumerBinding",
    "DecrementHit",
    "FinalizeResult",
    "FiredJoin",
    "ForkSpec",
    "JoinSpec",
    "NodeSpec",
    "_consumer_bindings",
    "_join_metadata",
    "_jsonb",
    "_metadata",
]


@dataclass(frozen=True, slots=True)
class ConsumerBinding:
    """A fired join's downstream consumer (dispatched as a normal step by
    the outbox drain — no hand-wired glue, no out-of-engine decode). The
    bindings are declared on the JoinSpec at fork time, stamped on the join
    row's metadata, and resolved into outbox rows at fire time."""

    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None = None
    map_index: int | None = None


@dataclass(frozen=True, slots=True)
class ChildSpec:
    """One fan-out child of a forking node. Ids are minted per fork by the
    engine (uuid7 via the seam) — the wiring lives in ``parent_id`` +
    ``map_index``, NEVER in a string-shape convention on hand-built ids."""

    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None = None
    map_index: int | None = None


@dataclass(frozen=True, slots=True)
class JoinSpec:
    """The join node a fork creates: born in join-wait with the declared
    parent count (its fan-out's children). Its ``consumers`` — the next
    steps whose existence awaits the join's output — ride the join row's
    metadata to the fire, which writes their outbox rows."""

    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None = None
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
    """One workflow node's enqueue (the workflow-row INSERT's spec).

    A JOINED node (``deps_pending > 0``) declares its incoming edges in
    ``parents`` — the edge ledger is the join counter's ONLY truth, and the
    public path writes them through the bundle's exported edge writer in
    the SAME call (a join whose edges never land is a stranded invisible
    join: the sweep diagnoses it ``orphan_parent``, and the declarative
    API's validators refuse it at build time)."""

    flow_id: JobId
    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None = None
    parent_id: JobId | None = None
    parents: tuple[JobId, ...] = ()
    map_index: int | None = None
    deps_pending: int = 0
    consumers: tuple[ConsumerBinding, ...] = ()
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
    consumers: tuple[ConsumerBinding, ...] = ()


@dataclass(frozen=True, slots=True)
class FinalizeResult:
    applied: bool
    attempt: int | None
    decremented: tuple[DecrementHit, ...] = ()
    fired: tuple[FiredJoin, ...] = ()


# ── The wire-shape helpers (one home; engine, fork and sweep import) ────


def _jsonb(value: object, *, default: str = "{}") -> str:
    """Encode a jsonb bind value (dict → str; None → *default* — the
    node-row payload columns are NOT NULL, so an absent payload binds the
    empty document, never NULL)."""
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return dumps_jsonb_str(value)


def _metadata(
    flow_id: JobId,
    *,
    blocking_reason: str | None,
    consumers: tuple[ConsumerBinding, ...] = (),
) -> dict[str, object]:
    """The node row's metadata: the flow link (every node carries it — the
    sweep's flow-status leg resolves it), the blocking reason, and the
    join's declared consumers (the fire resolves the outbox rows from
    them)."""
    meta: dict[str, object] = {"flow_id": str(flow_id)}
    if blocking_reason is not None:
        meta["blocking_reason"] = blocking_reason
    if consumers:
        meta["consumers"] = [
            {
                "step_key": c.step_key,
                "actor": c.actor,
                "queue": c.queue,
                "payload": c.payload,
                "map_index": c.map_index,
            }
            for c in consumers
        ]
    return meta


def _join_metadata(
    flow_id: JobId, consumers: tuple[ConsumerBinding, ...] = ()
) -> dict[str, object]:
    return _metadata(flow_id, blocking_reason=BLOCKING_REASON_JOIN, consumers=consumers)


def _consumer_bindings(raw: object) -> tuple[ConsumerBinding, ...]:
    """Decode the join row's metadata ``consumers`` array (the fork's
    declared bindings) into ConsumerBinding tuples. asyncpg returns jsonb
    as ``str`` on un-coded connections — parse before indexing."""
    if not raw:
        return ()
    decoded: Any = json.loads(raw) if isinstance(raw, str) else raw
    return tuple(
        ConsumerBinding(
            step_key=c["step_key"],
            actor=c["actor"],
            queue=c["queue"],
            payload=c.get("payload"),
            map_index=c.get("map_index"),
        )
        for c in decoded
    )
