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
from taskq.constants import BIGINT_MAX, SMALLINT_MAX
from taskq.retry import RetryPolicy
from taskq.workflows._capability import validate_required_worker_schema
from taskq.workflows._sql import BLOCKING_REASON_JOIN

if TYPE_CHECKING:
    from taskq.backend._protocol import ConnLike
    from taskq.workflows._policy import RetrySpec


#: The finite retry vocabulary a new workflow declaration may name. The
#: vanilla ``RetryKind`` also admits ``"indefinite"`` for previously
#: persisted rows and non-workflow actors; a workflow row carries no
#: ``schedule_to_close`` for that kind to stop at, so the authoring surface
#: names the finite set once here rather than re-spelling it at each door.
type FiniteRetryKind = Literal["transient", "non_retryable", "permanent"]
#: The stored form: the legacy ``"permanent"`` alias is normalized at the
#: declaration door, so nothing written to a row carries it.
type StoredFiniteRetryKind = Literal["transient", "non_retryable"]


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
    "FiniteRetryKind",
    "FiredJoin",
    "ForkSpec",
    "JoinSpec",
    "MapIndexSchemaError",
    "NodeSpec",
    "StoredFiniteRetryKind",
    "_consumer_binding",
    "_consumer_bindings",
    "_failure_info_from_json",
    "_join_metadata",
    "_jsonb",
    "_metadata",
    "_row_metadata",
    "_seconds_or_none",
    "duration_seconds",
    "resolve_map_index_ceiling",
    "resolve_map_index_domain",
    "validate_map_index_identity",
]

#: Conservative pre-migration ``map_index`` ceiling. It is deliberately not
#: a guess that every schema is smallint forever: only a HIGH candidate pays
#: for the all-five catalog read, which activates ``BIGINT_MAX`` after the
#: atomic POST migration and refuses mixed/corrupt schemas before the whole
#: authoring unit. No door may use this as the resolved bound for a
#: candidate above it.
MAP_INDEX_CEILING: Final[int] = SMALLINT_MAX

#: The five columns that jointly carry a non-null record identity. The
#: effective ceiling is the domain agreed by ALL of them: archive mirrors a
#: job, the outbox carries a fire's identity to the consumer insert, and
#: signals key their hold arbiter on the same value, so a widened ``jobs``
#: alone is not permission to write a high identity.
_MAP_INDEX_IDENTITY_TABLES: Final[tuple[str, ...]] = (
    "jobs",
    "jobs_archive",
    "wf_step_ledger",
    "wf_outbox",
    "wf_signals",
)

#: Exactly the two supported ``map_index`` column domains. Anything else
#: (``integer``, a domain type, an absent column) is a deployment/schema
#: error — never interpreted as permission to write the bigint domain.
_MAP_INDEX_COLUMN_BOUNDS: Final[dict[str, int]] = {
    "smallint": SMALLINT_MAX,
    "bigint": BIGINT_MAX,
}

#: One parameter-bound catalog read for all five carriers. The schema is the
#: caller's validated identifier (``$1``); the table set is ``$2``.
_MAP_INDEX_SCHEMA_SQL: Final[str] = (
    "SELECT c.table_name, c.data_type, c.is_nullable "
    "FROM information_schema.columns AS c "
    "JOIN pg_catalog.pg_namespace AS n ON n.nspname = c.table_schema "
    "JOIN pg_catalog.pg_class AS t "
    "ON t.relnamespace = n.oid AND t.relname = c.table_name "
    "JOIN pg_catalog.pg_attribute AS a "
    "ON a.attrelid = t.oid AND a.attname = c.column_name "
    "AND a.attnum > 0 AND NOT a.attisdropped "
    "JOIN pg_catalog.pg_type AS ty ON ty.oid = a.atttypid "
    "WHERE c.table_schema = $1 AND c.table_name = ANY($2::text[]) "
    "AND c.column_name = 'map_index' "
    "AND t.relkind = 'r' AND ty.typtype = 'b' AND ty.typbasetype = 0"
)


class MapIndexSchemaError(RuntimeError):
    """The schema cannot safely carry a non-null record identity.

    Raised when any of the five identity carriers is missing its
    ``map_index`` column, the carriers disagree on width, or a column has an
    unsupported type. The resolver never guesses bigint permission from a
    partial or hand-edited schema, and it never wraps a transport fault as a
    schema verdict: connection-level failures propagate unchanged so the
    ordinary retry/reclaim ladder owns them.
    """


def validate_map_index_identity(value: object, *, what: str = "map_index") -> int:
    """Refuse a non-null identity before any key render or SQL bind.

    The identity is an exact Python ``int`` (``bool`` is not an int here) and
    non-negative. The destination schema's upper bound is resolved later at
    the async DB door; this is the type/lower-bound half of the contract. No
    coercion: a float or string that merely looks numeric would otherwise
    format a different idempotency key or reach the SQL codec mid-transaction.
    """
    if type(value) is not int:
        raise ValueError(
            f"{what} must be an exact int (bool, float and string are refused; "
            f"no value coercion), got {type(value).__name__}"
        )
    if value < 0:
        raise ValueError(f"{what} must be non-negative, got {value}")
    return value


async def _map_index_columns(conn: ConnLike, schema: str) -> dict[str, str]:
    """Read and validate the five real base-table columns in one statement.

    The catalog joins deliberately exclude views, dropped columns and domains:
    their spelling in ``information_schema`` alone is not proof that the five
    physical application carriers have the supported shape. Nullability is part
    of that shape because the migration must preserve NULL ordinary identities.
    """
    rows = await conn.fetch(
        _MAP_INDEX_SCHEMA_SQL,
        schema,
        list(_MAP_INDEX_IDENTITY_TABLES),
    )

    found: dict[str, str] = {}
    for row in rows:
        table = row["table_name"]
        data_type = row["data_type"]
        nullable = row["is_nullable"]
        if (
            not isinstance(table, str)
            or not isinstance(data_type, str)
            or not isinstance(nullable, str)
        ):
            raise MapIndexSchemaError(
                f"the map_index catalog answer for schema {schema!r} is malformed: {row!r}"
            )
        if table in found:
            raise MapIndexSchemaError(
                f"the map_index catalog answer for schema {schema!r} is "
                f"ambiguous on {table!r}: duplicate physical column answers"
            )
        if nullable != "YES":
            raise MapIndexSchemaError(
                f"schema {schema!r} has an unsupported NOT NULL map_index identity "
                f"carrier {table!r}; the five carriers must remain nullable"
            )
        found[table] = data_type

    missing = [table for table in _MAP_INDEX_IDENTITY_TABLES if table not in found]
    if missing:
        raise MapIndexSchemaError(
            f"schema {schema!r} is missing the map_index identity column on "
            f"{missing!r}; all five real base-table carriers are required before "
            "a non-null record identity can be written"
        )
    unsupported = {
        table: data_type
        for table, data_type in found.items()
        if data_type not in _MAP_INDEX_COLUMN_BOUNDS
    }
    if unsupported:
        raise MapIndexSchemaError(
            f"schema {schema!r} has an unsupported map_index type on "
            f"{unsupported!r}; only smallint and bigint base types are valid "
            "identity carriers (integer, domains, views and other shapes are "
            "refused rather than guessed)"
        )
    return found


async def resolve_map_index_ceiling(conn: ConnLike, schema: str) -> int:
    """Return the minimum upper bound over all five identity carriers.

    This diagnostic deliberately retains the minimum on a mixed-width schema
    (the inspection contract). Authoring doors must instead use
    :func:`resolve_map_index_domain`, which rejects that mixed state rather
    than treating it as the supported all-smallint schema.
    """
    found = await _map_index_columns(conn, schema)
    return min(_MAP_INDEX_COLUMN_BOUNDS[found[table]] for table in _MAP_INDEX_IDENTITY_TABLES)


async def resolve_map_index_domain(conn: ConnLike, schema: str) -> int:
    """Resolve the activated authoring domain from one five-carrier read.

    Exactly all-smallint and exactly all-bigint are supported. Mixed widths
    are a typed deployment/schema error even though the diagnostic minimum
    helper above returns 32767 for them. Both functions share the same one
    statement; neither caches an answer or infers permission from a subset of
    carriers.

    A live read that observes a different width than the cached emit-capability
    verdict invalidates that verdict, so no worker can keep serving a stale
    pre-POST OK once it re-reads the carriers. The high-identity authoring
    doors call this resolver directly, and the bounded-emit preflight
    (:func:`taskq.workflows._capability.verify_emit_capacity_objects`) calls it
    at entry on every call, so a low-only worker that never authors a high
    identity still re-reads the five carriers before serving its cache.
    """
    from taskq.workflows._capability import note_emit_carrier_width

    found = await _map_index_columns(conn, schema)
    widths = {found[table] for table in _MAP_INDEX_IDENTITY_TABLES}
    if len(widths) != 1:
        raise MapIndexSchemaError(
            f"schema {schema!r} has partially widened/mixed map_index identity "
            f"carriers: {found!r}; apply the atomic five-carrier POST migration "
            "or restore a consistent schema. Until the five carriers agree, the "
            "high-identity authoring doors AND every bounded-emit preflight "
            "refuse — including a low-identity run's create and drive adoption"
        )
    width = next(iter(widths))
    note_emit_carrier_width(schema, width)
    return _MAP_INDEX_COLUMN_BOUNDS[width]


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

    def __post_init__(self) -> None:
        if self.map_index is not None:
            validate_map_index_identity(self.map_index)


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
    def resolved_retry_kind(self) -> StoredFiniteRetryKind | None:
        # RetrySpec.__post_init__ refuses "indefinite", so a constructed
        # spec's policy kind always lands in the stored finite vocabulary.
        return (
            cast("StoredFiniteRetryKind", self.retry.policy.kind)
            if self.retry is not None
            else None
        )

    def __post_init__(self) -> None:
        if self.map_index is not None:
            validate_map_index_identity(self.map_index)


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

    def __post_init__(self) -> None:
        validate_map_index_identity(self.map_index)


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
    retry_kind: FiniteRetryKind = "transient"
    #: the fork's JOIN is the source's own cohort (the placement fields say
    #: so), so its row lands at the source's resolution of the precedence law —
    #: the arms' overrides apply to the arm children, never the join. ``None``
    #: = the column default (the insert coalesces it to 0).
    priority: int | None = None
    #: the fork's JOIN is the source's own cohort — its row lands at the
    #: source's resolution (the arms' declarations govern the arm children,
    #: never the join). ``None`` = nothing declared — the column NULL.
    start_to_close: timedelta | None = None

    def __post_init__(self) -> None:
        from taskq.workflows._policy import declared_retry_kind

        object.__setattr__(
            self, "retry_kind", declared_retry_kind(self.retry_kind, what="ForkSpec.retry_kind")
        )


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
    retry_kind: FiniteRetryKind = "transient"
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
        from taskq.workflows._policy import declared_retry_kind

        if self.map_index is not None:
            validate_map_index_identity(self.map_index)
        if self.required_worker_schema is not None:
            validate_required_worker_schema(self.required_worker_schema)
        if self.retry is not None:
            policy = self.retry.policy
            if self.max_attempts != 3 and self.max_attempts != policy.max_attempts:
                raise ValueError("NodeSpec max_attempts contradicts its resolved retry policy")
            if (
                self.retry_kind != "transient"
                and declared_retry_kind(self.retry_kind, what="NodeSpec.retry_kind") != policy.kind
            ):
                raise ValueError("NodeSpec retry_kind contradicts its resolved retry policy")
            object.__setattr__(self, "max_attempts", policy.max_attempts)
            object.__setattr__(self, "retry_kind", policy.kind)
        else:
            object.__setattr__(
                self, "retry_kind", declared_retry_kind(self.retry_kind, what="NodeSpec.retry_kind")
            )


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
    #: The one-bit §4.1 result: True only when this terminal's event rule
    #: crossed the threshold and created the durable promotion intent (the
    #: direct route's acknowledged post-COMMIT drain trigger).
    intent_created: bool = False


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
    return tuple(_consumer_binding(c) for c in decoded)  # pyright: ignore[reportUnknownArgumentType]  # Why: the Any-contract walk — decoded's members are Unknown; _consumer_binding validates the required keys and the structured sub-docs (KeyError/ValueError on a malformed stamp) rather than asserting the shape away.


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


def _required_str(raw: dict[str, Any], key: str) -> str:
    """One REQUIRED binding field: a stamp that lacks the key raises
    ``KeyError``, and a present-but-wrong-typed value raises ``ValueError``
    — the persisted shape is validated at this boundary, never degraded."""
    value = raw[key]
    if not isinstance(value, str):
        raise ValueError(f"workflow consumer binding {key!r} must be a string")
    return value


def _consumer_binding(raw: dict[str, Any]) -> ConsumerBinding:
    """one metadata ``consumers`` entry → the typed binding (the walk's
    own arm).

    The three REQUIRED keys — ``step_key``, ``actor``, ``queue`` — are read
    directly (:func:`_required_str`): a stamp missing one raises
    ``KeyError`` and a wrong-typed one raises ``ValueError``. The
    structured sub-docs validate through their own readers
    (``rate_limits``/``reservations`` via :func:`_string_tuple`, ``retry``
    via :func:`_retry_from_doc`), so a malformed entry raises there too.
    ONLY the declared deadline degrades: ``start_to_close_s`` is a duration
    hint parsed back from the jsonb wire form
    (:func:`_consumer_binding_doc`'s serialization), so a malformed value
    becomes ``None`` (:func:`_seconds_or_none`) rather than a raise.

    This is a CROSS-RELEASE parse — since #745 a ``map_join``-naming
    ``consumers`` stamp can only be written by an older release into the
    same database — so the raise is a loud refusal at a real boundary, not
    a sweep death the reader is promised cannot happen."""
    return ConsumerBinding(
        step_key=_required_str(raw, "step_key"),
        actor=_required_str(raw, "actor"),
        queue=_required_str(raw, "queue"),
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
