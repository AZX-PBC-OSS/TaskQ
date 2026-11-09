"""The workflow engine's data types: the specs the enqueue/fork/finalize
paths carry, and the results they return. One home for the wire shapes —
the engine, the fork, and the sweep each import from here, and the API
composes them without re-deriving.

Id discipline: every ``*_id: JobId`` is uuid7 via the ``taskq._ids`` seam —
never DB-side or random-UUID generation (the TID251 ban).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Literal, cast

from taskq._json import dumps_jsonb_str
from taskq._json import loads as _json_loads
from taskq.backend._protocol import ErrorInfo, JobId, RetryKind
from taskq.retry import RetryPolicy
from taskq.workflows._capability import validate_required_worker_schema
from taskq.workflows._sql import BLOCKING_REASON_JOIN

if TYPE_CHECKING:
    from taskq.workflows._policy import RetrySpec


def duration_seconds(value: timedelta | float | int | None) -> float | None:
    """one normalizer for the second-denominated terms: a
    :class:`datetime.timedelta` or a bare number of seconds — the seconds
    float the row stamps and the sweep arithmetic read. ``None`` rides
    (nothing declared). The dual form is accepted at every
    duration-bearing term's face; the normalized seconds is the only
    stored form."""
    if value is None:
        return None
    if isinstance(value, timedelta):
        return value.total_seconds()
    return float(value)


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
    "_consumer_binding",
    "_consumer_bindings",
    "_failure_info_from_json",
    "_join_metadata",
    "_jsonb",
    "_metadata",
    "_row_metadata",
    "_seconds_or_none",
    "duration_seconds",
]

#: The map_index ceiling — the wire-format fact the ceiling error
#: documents: ``jobs.map_index`` is a smallint (the ledger's claim
#: arbiter column, 01.00.23_05), so a record's per-run identity tops out
#: at 32767. Without the door, record #32768 is the raw DataError the
#: smallint cast raises mid-transaction, taking the valid page-mates down
#: with it (an accidental untyped ceiling — the whole page rolls back
#: and the raw error ladders as transient). The engine refuses the
#: poison record at the door (:class:`taskq.workflows._emit.
#: MapIndexExhaustedError` — named, loud, the record named): the valid
#: page-mates COMMIT (the batch semantics — a poison record kills
#: itself loudly, never its mates), and the deterministic death
#: terminalizes the source with the ceiling documented here.
MAP_INDEX_CEILING: Final[int] = 32767

#: A join edge's declared failure policy (the two semantics):
#:
#: * ``fail_closed`` (the default) — a parent's terminal failure fails the
#:   join closed: the joined node blocks (``blocking_reason='failed_parent'``
#:   naming the failed parent), the flow fails (§17.2's cascade), and the
#:   running peers are peer-cancelled (the record
#:   ``by='peer_failure', cascade_from=<node>``). The record never shows a
#:   hanging join.
#: * ``collect`` — child failures do not cascade: each child runs to its
#:   own terminal; at exhaustion the failure fans in as a
#:   :class:`FailureInfo` item and the join fires with the typed partial
#:   result.
FailurePolicy = Literal["fail_closed", "collect"]


#: The absorbing policies (the edges whose declared policy absorbs a
#: failure): ``collect`` and ``maybe``. A ``fail_closed``
#: edge absorbs NOTHING — the cascade owns its resolution — so the
#: envelope's policy marker can never claim it (the typed door: the
#: envelope must not lie about which policy ran).
AbsorbingPolicy = Literal["collect", "maybe"]


#: the full runtime vocabulary —
#: :data:`taskq.workflows.definitions.FAILURE_POLICIES`, the build-time
#: validator's own door. The wiring verbs annotate with this (never a bare
#: ``str``): a wrong literal is a checker error at the call site, not a
#: surprise at build. (``FailurePolicy`` above is the engine-envelope's subset
#: spelling; ``AbsorbingPolicy`` the absorbing pair; both are sub-vocabularies
#: of this one.)
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
    #: The consumer's declared incoming-edge failure policy (the map-join
    #: consumption fix): the fork writes the join→consumer edge with it —
    #: the downstream dispatches through the same edge-ledger door as any
    #: node result (the counter's decrement gates the claim), and the
    #: propagation reads the policy off the ledger when the JOIN itself
    #: terminal-fails.
    failure_policy: str = "fail_closed"
    #: The consumer's declared deadline (the start_to_close bound): the
    #: downstream node's own resolution rides the binding — the join row's
    #: metadata `consumers` array (jsonb, serialized as seconds) → the
    #: fire's outbox bindings → the drain's consumer INSERT stamps the
    #: row's existing ``jobs.start_to_close`` column. ``None`` = the
    #: consumer declares none — the row's column NULL (the pre-fix row;
    #: the binding omits the key, the shape byte-identical).
    start_to_close: timedelta | None = None
    rate_limits: tuple[str, ...] = ()
    reservations: tuple[str, ...] = ()
    priority: int | None = None
    retry: RetrySpec | None = None
    #: Internal, stable terminal-effect key. Ordinary consumers never carry it.
    effect_key: str | None = None

    @property
    def retry_policy(self) -> RetryPolicy | None:
        return self.retry.policy if self.retry is not None else None


@dataclass(frozen=True, slots=True)
class ChildSpec:
    """One fan-out child of a forking node. Ids are minted per fork by the
    engine (uuid7 via the seam) — the wiring lives in ``parent_id`` +
    ``map_index``, never in a string-shape convention on hand-built ids."""

    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None = None
    map_index: int | None = None
    #: The child's admission terms (the admission-terms surface): the
    #: buckets' names, stamped onto the child row's metadata — the claim
    #: path acquires them through the same registry + the same denial
    #: path the queue-concurrency fence rides. ``()`` = no bucket.
    rate_limits: tuple[str, ...] = ()
    #: The child's strict capacity terms (the reservations' own
    #: ride): the reservation names stamped onto the child row's metadata
    #: beside ``rate_limits`` — the claim path resolves them through the
    #: resolved registry (the instances live there) and acquires them
    #: with the same denial discipline. ``()`` = no reservation.
    reservations: tuple[str, ...] = ()
    #: The child's resolved priority (the priority surface): the
    #: ``jobs.priority`` column value the fork's INSERT stamps — the
    #: builder's own resolution of the precedence law (per-call > the
    #: arm's > the source node's > the column default). ``None`` = the
    #: column default (the insert coalesces it to 0 — the pre-fix
    #: byte-identical value).
    priority: int | None = None
    #: The child's declared deadline (the start_to_close bound): the
    #: ``jobs.start_to_close`` column value the fork's INSERT stamps — the
    #: builder's own resolution (the arm's own > the source node's > the
    #: row's NULL). ``None`` = nothing declared — the column NULL (the
    #: pre-fix row; the enforcement falls to the worker's
    #: ``default_start_to_close`` at claim time, the resolution chain's
    #: own tail).
    start_to_close: timedelta | None = None
    #: Per-child retry lowering; None preserves the ForkSpec compatibility terms.
    retry: RetrySpec | None = None

    @property
    def retry_policy(self) -> RetryPolicy | None:
        return self.retry.policy if self.retry is not None else None

    @property
    def resolved_max_attempts(self) -> int | None:
        return self.retry.policy.max_attempts if self.retry is not None else None

    @property
    def resolved_retry_kind(self) -> RetryKind | None:
        return self.retry.policy.kind if self.retry is not None else None


@dataclass(frozen=True, slots=True)
class EmitChild:
    """One chain-start row a streaming source's emit tx inserts.

    The :class:`ChildSpec` shape PLUS the per-record identity stamps the
    emit owns — both REQUIRED, not defaulted: the refuted-claim
    discipline (the measured 198 UniqueViolations). The certified fork's
    idempotency key and the step-ledger's arbiter discriminate siblings
    by ``map_index``; the per-record ``trace_id`` is the one-query
    lineage (the recorded failure-down by trace). The fork's ``trace_id`` is
    per-fork; the emit's is per-child — the stamp-at-emit rule."""

    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None
    #: The record's trace — stamped at emit, carried forward by every
    #: fork the chain's steps take (the lineage never forks from the
    #: parent's column).
    trace_id: str
    #: load-bearing. the fork's idempotency key and the ledger's claim arbiter
    #: both ride it — two records' children of one step are two rows, two keys,
    #: two claims, only when it is stamped per record at emit.
    map_index: int
    rate_limits: tuple[str, ...] = ()
    reservations: tuple[str, ...] = ()
    priority: int | None = None
    start_to_close: timedelta | None = None
    retry: RetrySpec | None = None

    @property
    def max_attempts(self) -> int | None:
        return self.retry.policy.max_attempts if self.retry is not None else None

    @property
    def retry_kind(self) -> RetryKind | None:
        return self.retry.policy.kind if self.retry is not None else None

    @property
    def retry_policy(self) -> RetryPolicy | None:
        return self.retry.policy if self.retry is not None else None


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
    #: The source row's already-resolved reservation names (the run's own
    #: cohort — the static insert resolved them from the run's input and
    #: the declared args): the join row's metadata carries them so a
    #: nested fan-out (a map/route over this join) reads the stamp and
    #: the depth-2 children ride the run's cohort — the guarantee is
    #: depth-N, not depth-1.
    reservations: tuple[str, ...] = ()
    #: The declared failure policy recorded on the join's incoming edges
    #: The declared policy: what a parent's terminal failure does to this join.
    failure_policy: FailurePolicy = "fail_closed"
    #: The child-driven escape: the join past the declared fan-in bound
    #: counts its terminal children FROM the edge ledger at fire time (the
    #: counter-as-cache is never trusted; the choice is recorded on the
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
    retry_kind: RetryKind | Literal["permanent"] = "transient"
    #: the fork's JOIN is the source's own cohort (the placement fields say
    #: so), so its row lands at the source's resolution of the precedence law —
    #: the arms' overrides apply to the arm children, never the join. ``None``
    #: = the column default (the insert coalesces it to 0).
    priority: int | None = None
    #: the fork's JOIN is the source's own cohort — its row lands at the
    #: source's resolution (the arms' declarations govern the arm children,
    #: never the join). ``None`` = nothing declared — the column NULL.
    start_to_close: timedelta | None = None


@dataclass(frozen=True, slots=True)
class NodeSpec:
    """One workflow node's enqueue (the workflow-row INSERT's spec).

    A JOINED node (``deps_pending > 0``) declares its incoming edges in
    ``parents`` — the edge ledger is the join counter's only truth, and the
    public path writes them through the bundle's exported edge writer in
    the same call (a join whose edges never land is a stranded invisible
    join: the sweep diagnoses it ``orphan_parent``, and the declarative
    API's validators refuse it at build time)."""

    flow_id: JobId
    step_key: str
    actor: str
    queue: str
    payload: dict[str, object] | None = None
    parent_id: JobId | None = None
    parents: tuple[JobId, ...] = ()
    #: The failure policy recorded on this node's incoming edges when it is
    #: a joined node (the public join path's declaration).
    failure_policy: FailurePolicy = "fail_closed"
    #: The child-driven escape (recorded on the joined node's metadata).
    child_driven: bool = False
    map_index: int | None = None
    deps_pending: int = 0
    consumers: tuple[ConsumerBinding, ...] = ()
    trace_id: str | None = None
    max_attempts: int = 3
    retry_kind: RetryKind | Literal["permanent"] = "transient"
    idempotency_scope: str = ""
    idempotency_key: str | None = None
    #: the ``jobs.priority`` column value the insert stamps — the resolved
    #: value (the caller's own precedence arithmetic: per-call > the
    #: declaration > the column default 0). The default is the column default's
    #: own value, so a spec built without it stores exactly the pre-fix row.
    priority: int = 0
    #: the ``jobs.start_to_close`` column value the insert stamps — the
    #: resolved value (the caller's own precedence arithmetic). The default is
    #: the column's own NULL (the pre-fix row: the column is nullable with no
    #: default — migrations/01.00.00_01:85), so a spec built without it stores
    #: exactly the pre-fix row.
    start_to_close: timedelta | None = None
    #: Internal guarded-run inheritance. Existing callers default to NULL and
    #: never stamp a capability accidentally; future feature fronts enter via
    #: the validated helper, while this model revalidates direct construction.
    required_worker_schema: int | None = None
    retry: RetrySpec | None = None

    @property
    def retry_policy(self) -> RetryPolicy | None:
        return self.retry.policy if self.retry is not None else None

    def __post_init__(self) -> None:
        from taskq.workflows._policy import canonical_retry_kind

        if self.required_worker_schema is not None:
            validate_required_worker_schema(self.required_worker_schema)
        if self.retry is not None:
            policy = self.retry.policy
            if self.max_attempts != 3 and self.max_attempts != policy.max_attempts:
                raise ValueError("NodeSpec max_attempts contradicts its resolved retry policy")
            if (
                self.retry_kind != "transient"
                and canonical_retry_kind(self.retry_kind) != policy.kind
            ):
                raise ValueError("NodeSpec retry_kind contradicts its resolved retry policy")
            object.__setattr__(self, "max_attempts", policy.max_attempts)
            object.__setattr__(self, "retry_kind", policy.kind)
        else:
            object.__setattr__(self, "retry_kind", canonical_retry_kind(self.retry_kind))


@dataclass(frozen=True, slots=True)
class DecrementHit:
    join_job_id: JobId
    deps_pending: int


@dataclass(frozen=True, slots=True)
class FailureInfo:
    """One collected child failure (the fan-in item).

    ENVELOPE IS the ESTATE'S, never re-spelled
    ``error`` IS :class:`taskq.backend._protocol.ErrorInfo` — the same
    typed envelope every terminal write uses, its bound constants
    (``ERROR_CLASS_MAX_CHARS`` et al.) carried as-is. ``attempts`` /
    ``node_key`` / ``map_index`` are the workflow-only extensions.

    ``attempts`` carries the FULL attempt history (one entry per ladder
    attempt, every error payload — the measured invariant); a
    SKIP's fan-in carries ``attempts=()`` (a skip is not an attempt —
    zero ledger rows).
    """

    node_key: str
    map_index: int | None
    error: ErrorInfo
    attempts: tuple[tuple[int, str | None, str | None], ...] = ()
    #: The edge's own declared policy that absorbed this failure (the
    #: the envelope must not lie about which policy ran — 'collect' and
    #: 'maybe' absorb; the marker is on the item, typed to the absorbing
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
    # The policy is the typed absorbing vocabulary: the walk repairs a
    # pre-policy item (no policy key → 'collect' — the item predates the
    # marker) and refuses to carry an out-of-vocabulary value through
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
    node-row payload columns are not NULL, so an absent payload binds the
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
        meta["consumers"] = [_consumer_binding_doc(c) for c in consumers]
    return meta


def _consumer_binding_doc(c: ConsumerBinding) -> dict[str, object]:
    """one consumer binding's jsonb shape (the join row's metadata
    ``consumers`` array entry — the fire reads them back through
    :func:`_consumer_bindings` and copies them into the outbox
    bindings): the placement + payload + map index, PLUS the declared
    deadline as SECONDS when the consumer declared one (the wf
    start_to_close fix — jsonb has no interval; seconds is the wire
    form the drain parses back). The undeclared consumer omits the key
    — the pre-fix shape byte-identical."""
    doc: dict[str, object] = {
        "step_key": c.step_key,
        "actor": c.actor,
        "queue": c.queue,
        "payload": c.payload,
        "map_index": c.map_index,
    }
    if c.start_to_close is not None:
        doc["start_to_close_s"] = c.start_to_close.total_seconds()
    if c.rate_limits:
        doc["rate_limits"] = list(c.rate_limits)
    if c.reservations:
        doc["reservations"] = list(c.reservations)
    if c.priority is not None:
        doc["priority"] = c.priority
    if c.effect_key is not None:
        doc["wf_effect_key"] = c.effect_key
        doc["after_terminal"] = True
    if c.retry is not None:
        policy = c.retry.policy
        doc["retry"] = {
            "max_attempts": policy.max_attempts,
            "kind": policy.kind,
            "base_s": policy.base.total_seconds(),
            "cap_s": policy.cap.total_seconds(),
            "backoff": policy.backoff,
            "jitter": policy.jitter,
            **(
                {"non_consuming_limit": c.retry.non_consuming_limit}
                if c.retry.non_consuming_limit is not None
                else {}
            ),
        }
    return doc


def _row_metadata(
    flow_id: JobId,
    *,
    rate_limits: tuple[str, ...] = (),
    reservations: tuple[str, ...] = (),
    retry: RetrySpec | None = None,
) -> dict[str, object]:
    """The node row's metadata WITH admission terms (the admission terms): the base
    flow link plus the ``rate_limits`` names and the STRICT reservation
    NAMES (the reservations' own ride) when the node declares
    them — the row its own admission terms, the claim path's read.
    Both empty = the base metadata, byte-identical (the no-terms rows
    never change shape)."""
    meta = _metadata(flow_id, blocking_reason=None)
    if rate_limits:
        meta["rate_limits"] = list(rate_limits)
    if reservations:
        meta["reservations"] = list(reservations)
    if retry is not None and retry.non_consuming_limit is not None:
        meta["wf_retry"] = {"non_consuming_limit": retry.non_consuming_limit}
    return meta


def _join_metadata(
    flow_id: JobId,
    consumers: tuple[ConsumerBinding, ...] = (),
    *,
    child_driven: bool = False,
    reservations: tuple[str, ...] = (),
) -> dict[str, object]:
    """The joined node's metadata: the flow link, the join-wait marker, the
    declared consumers — and the shape record (metadata.join_shape) when
    the child-driven escape declared it: the choice rides the ROW (the
    sweep's re-derive reads it; the record never guesses). The SOURCE
    ROW's already-resolved reservation names ride beside them (the run's
    own cohort): a nested fan-out over this join reads the stamp, the
    depth-2 children inherit the run's cap — the cohort guarantee is
    depth-N."""
    meta = _metadata(flow_id, blocking_reason=BLOCKING_REASON_JOIN, consumers=consumers)
    if child_driven:
        meta["join_shape"] = "child_driven"
    if reservations:
        meta["reservations"] = list(reservations)
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
    return tuple(_consumer_binding(c) for c in decoded)  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk — decoded's members are Unknown; _consumer_binding's asserts guard the runtime shape.


def _seconds_or_none(raw: object) -> timedelta | None:
    """The binding's declared SECONDS → the deadline timedelta (the jsonb
    wire form's one parse — :func:`_consumer_binding`'s decode and the
    drain's consumer INSERT are its two faces): a non-number or a
    negative degrades to None (a declaration is never a correctness
    input worth a sweep death)."""
    return timedelta(seconds=float(raw)) if isinstance(raw, (int, float)) and raw >= 0 else None


def _string_tuple(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError("workflow consumer admission names must be a list of strings")
    values = cast("list[object]", raw)
    if not all(isinstance(value, str) for value in values):
        raise ValueError("workflow consumer admission names must be a list of strings")
    return tuple(cast("list[str]", values))


def _retry_from_doc(raw: object) -> RetrySpec | None:
    """Validate a persisted consumer's execution terms before interpreting them."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("workflow consumer retry must be an object")
    document = cast("dict[str, object]", raw)
    from taskq.workflows._policy import RetrySpec, canonical_retry_kind

    kind = document.get("kind", "transient")
    backoff = document.get("backoff", "exponential")
    if not isinstance(kind, str) or backoff not in ("exponential", "linear", "fixed"):
        raise ValueError("workflow consumer retry has an invalid kind/backoff")
    numeric: dict[str, float] = {}
    for key in ("base_s", "cap_s", "jitter"):
        value = document.get(key)
        if type(value) not in (int, float):
            raise ValueError(f"workflow consumer retry {key} must be numeric")
        numeric[key] = float(cast("int | float", value))
    max_attempts = document.get("max_attempts", 3)
    if type(max_attempts) is not int:
        raise ValueError("workflow consumer retry max_attempts must be an integer")
    limit = document.get("non_consuming_limit")
    return RetrySpec(
        policy=RetryPolicy(
            max_attempts=max_attempts,
            kind=canonical_retry_kind(kind, what="consumer retry kind"),
            base=timedelta(seconds=numeric["base_s"]),
            cap=timedelta(seconds=numeric["cap_s"]),
            backoff=backoff,
            jitter=numeric["jitter"],
        ),
        non_consuming_limit=limit if type(limit) is int else None,
    )


def _consumer_binding(raw: dict[str, Any]) -> ConsumerBinding:
    """one metadata ``consumers`` entry → the typed binding (the walk's
    own arm): the declared deadline parses back from the binding's
    SECONDS (the jsonb wire form — :func:`_consumer_binding_doc`'s own
    serialization); a malformed value degrades to None (the binding is
    a declaration, never a correctness input worth a sweep death)."""
    return ConsumerBinding(
        step_key=raw["step_key"],  # pyright: ignore[reportUnknownArgumentType,reportIndexType]  # Why: the Any-contract walk (the ledger wrote the shape).
        actor=raw["actor"],  # pyright: ignore[reportUnknownArgumentType,reportIndexType]
        queue=raw["queue"],  # pyright: ignore[reportUnknownArgumentType,reportIndexType]
        payload=raw.get("payload"),
        map_index=raw.get("map_index"),
        start_to_close=_seconds_or_none(raw.get("start_to_close_s")),
        rate_limits=_string_tuple(raw.get("rate_limits")),
        reservations=_string_tuple(raw.get("reservations")),
        priority=raw.get("priority") if type(raw.get("priority")) is int else None,
        retry=_retry_from_doc(raw.get("retry")),
        effect_key=(
            raw.get("wf_effect_key")
            if raw.get("after_terminal") is True
            and isinstance(raw.get("wf_effect_key"), str)
            and raw.get("wf_effect_key")
            else None
        ),
    )
