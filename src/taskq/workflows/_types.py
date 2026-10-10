"""The workflow engine's data types (T04): the specs the enqueue/fork/finalize
paths carry, and the results they return. One home for the wire shapes —
the engine, the fork, and the sweep each import from here, and T09's API
composes them without re-deriving.

Id discipline: every ``*_id: JobId`` is uuid7 via the ``taskq._ids`` seam —
never DB-side or random-UUID generation (the TID251 ban).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Literal

from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ErrorInfo, JobId
from taskq.workflows._sql import BLOCKING_REASON_JOIN

__all__ = [
    "MAP_INDEX_CEILING",
    "AbsorbingPolicy",
    "ChildSpec",
    "ConsumerBinding",
    "DecrementHit",
    "EdgeFailurePolicy",
    "EmitChild",
    "FailureInfo",
    "FailurePolicy",
    "FinalizeResult",
    "FiredJoin",
    "ForkSpec",
    "JoinSpec",
    "NodeSpec",
    "_consumer_bindings",
    "_failure_info_from_json",
    "_join_metadata",
    "_jsonb",
    "_metadata",
    "_row_metadata",
]

#: THE MAP_INDEX CEILING — the wire-format fact the ceiling error
#: documents: ``jobs.map_index`` is a SMALLINT (the ledger's claim
#: arbiter column, 01.00.23_05), so a record's per-run identity tops out
#: at 32767. Record #32768 is the convicted shape: the raw DataError the
#: smallint cast raised MID-TRANSACTION took the valid page-mates down
#: with it (an accidental untyped ceiling — the whole page rolled back
#: and the raw error laddered as transient). The engine now refuses the
#: poison record AT THE DOOR (:class:`taskq.workflows._emit.
#: MapIndexExhaustedError` — named, loud, the record named): the valid
#: page-mates COMMIT (the batch semantics — a poison record kills
#: ITSELF loudly, never its mates), and the deterministic death
#: terminalizes the source with the ceiling documented here.
MAP_INDEX_CEILING: Final[int] = 32767

#: A join edge's declared failure policy (T06's two semantics):
#:
#: * ``fail_closed`` (the default) — a parent's TERMINAL failure fails the
#:   join closed: the joined node blocks (``blocking_reason='failed_parent'``
#:   naming the failed parent), the flow fails (§17.2's cascade), and the
#:   running peers are peer-cancelled (the record
#:   ``by='peer_failure', cascade_from=<node>``). The record never shows a
#:   hanging join.
#: * ``collect`` — child failures do NOT cascade: each child runs to its
#:   own terminal; at exhaustion the failure fans in as a
#:   :class:`FailureInfo` item and the join FIRES with the typed partial
#:   result.
FailurePolicy = Literal["fail_closed", "collect"]


#: The ABSORBING policies (the edges whose declared policy absorbs a
#: failure): T06's ``collect`` and T07's ``maybe``. A ``fail_closed``
#: edge absorbs NOTHING — the cascade owns its resolution — so the
#: envelope's policy marker can never claim it (the typed door: the
#: envelope must not lie about which policy ran, T07's C).
AbsorbingPolicy = Literal["collect", "maybe"]


#: THE EDGE POLICY (the wiring verbs' ``on_failure=``): the FULL runtime
#: vocabulary — :data:`taskq.workflows.definitions.FAILURE_POLICIES`, the
#: build-time validator's own door. The wiring verbs annotate with THIS
#: (never a bare ``str``): a wrong literal is a checker error at the
#: call site, not a surprise at build. (``FailurePolicy`` above is the
#: engine-envelope's subset spelling; ``AbsorbingPolicy`` the absorbing
#: pair; both are sub-vocabularies of this one.)
EdgeFailurePolicy = Literal["fail_closed", "collect", "maybe"]


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
    #: The CONSUMER's declared incoming-edge failure policy (the map-join
    #: consumption cure): the fork writes the join→consumer EDGE with it —
    #: the downstream dispatches through the SAME edge-ledger door as any
    #: node result (the counter's decrement gates the claim), and T06's
    #: propagation reads the policy off the ledger when the JOIN itself
    #: terminal-fails.
    failure_policy: str = "fail_closed"


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
    #: The child's ADMISSION TERMS (the consumer-face lane's CURE 2): the
    #: buckets' NAMES, stamped onto the child row's metadata — the claim
    #: path acquires them through the same registry + the same denial
    #: path the queue-concurrency fence rides. ``()`` = no bucket.
    rate_limits: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EmitChild:
    """One chain-start row a streaming source's emit tx inserts (T20).

    The :class:`ChildSpec` shape PLUS the per-record identity stamps the
    emit owns — BOTH REQUIRED, not defaulted: the refuted-claim
    discipline (the spike's 198 UniqueViolations). The certified fork's
    idempotency key AND the step-ledger's arbiter discriminate siblings
    by ``map_index``; the per-record ``trace_id`` is the one-query
    lineage (the drill-down by trace). The fork's ``trace_id`` is
    per-fork; the emit's is per-child — the stamp-at-emit rule."""

    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None
    #: The record's trace — stamped at emit, carried forward by every
    #: fork the chain's steps take (the lineage never forks from the
    #: parent's column).
    trace_id: str
    #: THE DISCRIMINATOR (load-bearing): the fork's idempotency key and
    #: the ledger's claim arbiter both ride it — two records' children of
    #: one step are two rows, two keys, two claims, only when it is
    #: stamped per record at emit.
    map_index: int


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
    #: The declared failure policy recorded on the join's incoming edges
    #: (T06): what a parent's TERMINAL failure does to this join.
    failure_policy: FailurePolicy = "fail_closed"
    #: T07's child-driven escape: the join past the declared fan-in bound
    #: counts its terminal children FROM THE EDGE LEDGER at fire time (the
    #: counter-as-cache is never trusted; the choice is RECORDED on the
    #: joined node's metadata — metadata.join_shape). The declared-edge
    #: shape (the default) is the bounded one.
    child_driven: bool = False


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
    #: The failure policy recorded on THIS node's incoming edges when it is
    #: a joined node (the public join path's declaration — T06).
    failure_policy: FailurePolicy = "fail_closed"
    #: T07's child-driven escape (recorded on the joined node's metadata).
    child_driven: bool = False
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
class FailureInfo:
    """One collected child failure (T06's Item failure — the fan-in item).

    THE ENVELOPE IS THE ESTATE'S, never re-spelled (GAPS-ESTATE F9):
    ``error`` IS :class:`taskq.backend._protocol.ErrorInfo` — the same
    typed envelope every terminal write uses, its bound constants
    (``ERROR_CLASS_MAX_CHARS`` et al.) carried as-is. ``attempts`` /
    ``node_key`` / ``map_index`` are the workflow-only extensions.

    ``attempts`` carries the FULL attempt history (one entry per ladder
    attempt, every error payload — P3 spike3's measured invariant); a
    SKIP's fan-in carries ``attempts=()`` (a skip is not an attempt —
    zero ledger rows).
    """

    node_key: str
    map_index: int | None
    error: ErrorInfo
    attempts: tuple[tuple[int, str | None, str | None], ...] = ()
    #: The edge's OWN declared policy that absorbed this failure (T07's C:
    #: the envelope must not lie about which policy ran — 'collect' and
    #: 'maybe' absorb; the marker is on the item, TYPED to the absorbing
    #: vocabulary — a fail_closed claim is unconstructible at this door).
    policy: AbsorbingPolicy = "collect"

    def to_json(self) -> dict[str, object]:
        """The fan-in item's jsonb shape (the join row's ``failures``
        array entry — the shape :func:`_failure_info_from_json` reads
        back; one home for the wire shape)."""
        return {
            "node_key": self.node_key,
            "map_index": self.map_index,
            "policy": self.policy,
            "error": {
                "error_class": self.error.error_class,
                "error_message": self.error.error_message,
                "error_traceback": self.error.error_traceback,
            },
            "attempts": [
                {"attempt": a, "error_class": ec, "error_message": em}
                for a, ec, em in self.attempts
            ],
        }


def _failure_info_from_json(raw: object) -> FailureInfo:
    """Decode one fan-in item's jsonb (the typed read-side door — the
    admin/collector surfaces consume the TYPED shape, never a bare dict)."""
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
    assert isinstance(decoded, dict)
    # The walk repairs caller-agnostic JSON values (the _json.py walk's
    # Any-contract): every branch asserts the runtime shape it consumes.
    err_raw: Any = decoded.get("error") or {}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]  # Why: decoded is Any by the parse contract; the asserts below are the runtime shape guards.
    assert isinstance(err_raw, dict)
    attempts_raw: Any = decoded.get("attempts") or []  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]  # Why: same Any-contract walk.
    assert isinstance(attempts_raw, list)
    attempts: list[tuple[int, str | None, str | None]] = []
    for entry in attempts_raw:  # pyright: ignore[reportUnknownVariableType]  # Why: list membership is Unknown under the Any-contract; the assert is the guard.
        assert isinstance(entry, dict)
        attempts.append(
            (
                int(entry["attempt"]),  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: same Any-contract walk; the ledger wrote ints.
                entry.get("error_class"),  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: same walk.
                entry.get("error_message"),  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: same walk.
            )
        )
    traceback_raw: Any = err_raw.get("error_traceback")  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]  # Why: same Any-contract walk.
    # The POLICY is the typed absorbing vocabulary: the walk repairs a
    # pre-policy item (no policy key → 'collect' — the item predates the
    # marker) and refuses to carry an OUT-OF-VOCABULARY value through
    # (a corrupt item reads as the default, never as a lie).
    policy_raw: Any = decoded.get("policy", "collect")  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]  # Why: same Any-contract walk.
    policy: AbsorbingPolicy = policy_raw if policy_raw in ("collect", "maybe") else "collect"
    return FailureInfo(
        node_key=str(decoded.get("node_key", "")),  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: the Any-contract walk (decoded's members are Unknown); the asserts above guard the runtime shape.
        map_index=decoded.get("map_index"),  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: same walk.
        error=ErrorInfo(
            error_class=str(err_raw.get("error_class", "")),  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: same walk.
            error_message=str(err_raw.get("error_message", "")),  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: same walk.
            error_traceback=traceback_raw if isinstance(traceback_raw, str) else None,
        ),
        attempts=tuple(attempts),
        policy=policy,
    )


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


def _row_metadata(
    flow_id: JobId,
    *,
    rate_limits: tuple[str, ...] = (),
) -> dict[str, object]:
    """The node row's metadata WITH admission terms (CURE 2): the base
    flow link plus the ``rate_limits`` names when the node declares
    buckets — the row its own admission terms, the claim path's read.
    ``rate_limits`` empty = the base metadata, byte-identical (the
    no-buckets rows never change shape)."""
    meta = _metadata(flow_id, blocking_reason=None)
    if rate_limits:
        meta["rate_limits"] = list(rate_limits)
    return meta


def _join_metadata(
    flow_id: JobId,
    consumers: tuple[ConsumerBinding, ...] = (),
    *,
    child_driven: bool = False,
) -> dict[str, object]:
    """The joined node's metadata: the flow link, the join-wait marker, the
    declared consumers — and T07's shape record (metadata.join_shape) when
    the child-driven escape declared it: the choice rides the ROW (the
    sweep's re-derive reads it; the record never guesses)."""
    meta = _metadata(flow_id, blocking_reason=BLOCKING_REASON_JOIN, consumers=consumers)
    if child_driven:
        meta["join_shape"] = "child_driven"
    return meta


def _consumer_bindings(raw: object) -> tuple[ConsumerBinding, ...]:
    """Decode the join row's metadata ``consumers`` array (the fork's
    declared bindings) into ConsumerBinding tuples. asyncpg returns jsonb
    as ``str`` on un-coded connections — parse before indexing."""
    if not raw:
        return ()
    # asyncpg returns jsonb as ``str`` on un-coded connections — parse
    # before indexing (the estate's _json seam, never the stdlib import).
    decoded: Any = _json_loads(raw) if isinstance(raw, str) else raw
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
