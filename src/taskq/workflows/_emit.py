"""the EMIT TX — the streaming source's per-page statement group,
the design's one new primitive (PROOF.md §6a).

The shipped fork inserts children only inside the parent's terminal-mark
tx1 (fork-at-finalize): a child's existence is INDEXED to its parent's
death. A STREAMING source must do the opposite — emit children while
ITSELF staying ``running`` — so its pages stream (children claim +
terminal while the source is mid-fetch; the spike measured 236 chain
claims while the source was still streaming).

EMIT TX, one transaction
  1. the children INSERT   (the certified FORK_CHILDREN shape re-bound)
  2. the edge rows         (the certified FORK_EDGES shape re-bound —
                           child→source, fail_closed: a mid-stream source
                           death is the honest fail-closed parent)
  3. the CURSOR CHECKPOINT (the source row's own metadata under the FULL
                           claim fence — no new table, no new column)

Atomicity is the resume invariant: a kill at any statement window —
including after the cursor write but before the commit — rolls the whole
tx back; the reclaim re-pends the source (it never finalized), the
re-claim re-emits exactly the lost page, never a partial one, never a
duplicate one. the FENCE IS the CLAIM: a zombie source (its attempt
superseded) updates nothing in step 3 → :class:`EmitFencedError` → the
children and edges roll back with the tx — a zombie cannot emit.

The five identity carriers determine the active upper bound: 32767 before
their atomic POST widening, BIGINT_MAX (9223372036854775807) when all five
are bigint. A high candidate pays for one all-five catalog statement; an
ordinary low page's catalog cost follows the declared bound — a BOUNDED LOW
page (reserved replay or unreserved admission) runs the five-carrier
capability preflight before any write and refuses a missing, mixed or botched
shape with :class:`WorkflowWorkerCapabilityError`, while an explicit unbounded
(``max_in_flight=None``) low page deliberately pays no catalog read (a high
candidate's authoring-door resolve refuses a missing or mixed shape as
:class:`MapIndexSchemaError` first). A page
whose record crosses the resolved
bound raises :class:`MapIndexExhaustedError` at the emit door; valid
page-mates commit first (the poison record kills itself, never its mates).
Missing, unknown and mixed carrier shapes instead refuse the whole batch as
:class:`MapIndexSchemaError`. The bound is per-map-index, not per-run and
not a fan-in counter bound.

``emit_batch`` accepts ``_window_hook``
(the attack suite's seam), fired
after EACH statement with ``emit:{1|2|3}``; the pins terminate the
backend there (a real server-side kill) and prove zero re-emitted, zero
lost children.

The MAX-IN-FLIGHT bound is DECLARED at the workflow level (the runner
supplies the workflow's declaration; ``None`` = explicit unbounded). Every
whole page passes the same admission lock and derived observation immediately
before its write. Ordinary saturation raises :class:`EmitBackpressure`: it
writes no page and the runner parks the claimed source on the exact marker
with the park instant. Live rows remain the only current-status and capacity
authority — there is no persisted charge, waiter count, cached head, or
cached minimum width to drift.

Wake is a threshold-created durable intent plus an event-driven drain, never
a jobs trigger or a polling loop. Each terminal/park event runs one indexed
exact-marker probe (a park runs one shared observation instead), and only a
marker-positive event that newly fits derives capacity and creates the
durable promotion intent with its accelerator notification. The drain — the
runner's own commit-acknowledged call, the maintenance leader, or the
bounded recovery arms — selects the oldest fitting real waiter and attaches
a durable page-width reservation, so a woken source cannot be barged into
another no-fit re-park. A replay may consume less than its reservation; it
may not exceed it (:class:`EmitReservationExceededError`). Durable rows, not
NOTIFY, are authority.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

import structlog

from taskq._ids import new_uuid
from taskq._json import loads as _json_loads
from taskq.backend._protocol import JobId
from taskq.constants import wake_channel, wf_emit_capacity_channel
from taskq.obs import get_logger, get_meter
from taskq.workflows._fork import FORK_CHUNK
from taskq.workflows._policy import canonical_retry_kind, row_retry_curve
from taskq.workflows._sql_finalize import WF_EMIT_ROTATION_BATCH
from taskq.workflows._types import (
    MAP_INDEX_CEILING,
    EmitChild,
    _jsonb,
    _row_metadata,
    resolve_map_index_domain,
)

if TYPE_CHECKING:
    import asyncpg

    from taskq.backend._protocol import ConnLike
    from taskq.workflows._sql import WorkflowSql

__all__ = [
    "EMIT_CURSOR_KEY",
    "EMIT_MAX_IN_FLIGHT_DEFAULT",
    "NOT_DRAINING_COUNTER",
    "EmitBackpressure",
    "EmitCancelledError",
    "EmitCapacityObservation",
    "EmitFencedError",
    "EmitReservationExceededError",
    "EmitStaleClaimError",
    "EmitWakeResult",
    "MapIndexExhaustedError",
    "PageDivergedError",
    "emit_batch",
    "wake_emit_backpressure",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

#: The §5.4 non-drainage refusal's own counter — the ONE declaration (the
#: maintenance seat imports this; two instruments with divergent attribute
#: streams would silently split the operator's alert). The attribute
#: contract is one truth: BOTH seats emit exactly ``error_class`` (always
#: the WORKFLOW_NOT_DRAINING_CLASS constant), so an alert/dashboard
#: filtering on it sees every refusal from either seat.
NOT_DRAINING_COUNTER = get_meter().create_counter(
    name="taskq.workflows.wf_not_draining",
    unit="{refusal}",
    description="The proven non-drainage refusals (the committed terminal-root "
    "proof behind an exhausted child failure, with an exact parked source "
    "still holding un-emitted work — including the first page when no "
    "cursor has committed).",
)


@dataclass(frozen=True, slots=True)
class EmitCapacityCounters:
    """The §4.2 operation-family census, live at the real sites.

    Every family the invocation-bound table names is counted exactly once
    per operation instance: ``admission_decisions`` (P + K: one whole-page
    admission decision each), ``source_body_claims`` (P + K + S + J + C),
    ``fitted_grants`` (G), ``new_intents`` (I), ``flow_drains`` (D: per
    ENTERED drain transaction, never per invocation — a chunked
    invocation contributes one D per entered chunk transaction,
    including a transaction that subsequently fails; pool-acquisition or
    transaction-entry failure is not a D), ``global_drains`` (U), and the
    read families ``probes`` (E_probe = T) and ``observations``
    (E_observation = X + K) plus ``candidate_reads`` (ACTUAL SQL query
    executions: each drain read point is one shared aggregate observation
    plus one fitting-candidate query, so the family is exactly
    ``2 * (G + D)`` for every ordinarily terminating drain; an exception
    before a read point can only lower it). No count is hidden inside
    another; the snapshot is the acceptance report's input."""

    admission_decisions: int = 0
    source_body_claims: int = 0
    fitted_grants: int = 0
    new_intents: int = 0
    flow_drains: int = 0
    global_drains: int = 0
    probes: int = 0
    observations: int = 0
    candidate_reads: int = 0
    parks: int = 0
    terminals: int = 0
    # THE M FAMILY (§4.2): the disclosed maintenance-interval recovery
    # arms' own reads/repairs — outside the healthy lane's V and E.
    maintenance_probes: int = 0
    maintenance_observations: int = 0
    maintenance_selections: int = 0
    maintenance_repairs: int = 0

    def snapshot(self) -> dict[str, int]:
        from dataclasses import asdict

        return {k: int(v) for k, v in asdict(self).items()}


#: The process census (the tests reset it between lanes; the product never
#: reads it for behavior).
COUNTERS = EmitCapacityCounters()


def bump_emit_census(name: str, by: int = 1) -> None:
    object.__setattr__(COUNTERS, name, getattr(COUNTERS, name) + by)


def reset_counters() -> dict[str, int]:
    """Zero the census and return the pre-reset snapshot (the lane's own
    seam; the product's behavior never reads it)."""
    before = COUNTERS.snapshot()
    for name in before:
        object.__setattr__(COUNTERS, name, 0)
    return before


#: The cursor checkpoint's metadata key on the source row itself (the
#: checkpoint rides the row's jsonb — no new table, no new column, no
#: migration). The value is the caller's cursor document, merged at the
#: top level of the row's metadata under this one key.
EMIT_CURSOR_KEY: Final[str] = "emit_cursor"

#: the bound a workflow gets when it does not declare one. The author opts out
#: loudly (``max_in_flight=None`` on the workflow declaration — the explicit
#: unbounded, never a silent default).
EMIT_MAX_IN_FLIGHT_DEFAULT: Final[int] = 1000

#: The window-hook type: fired after each in-tx statement with the
#: window's name (``emit:1`` children, ``emit:2`` edges, ``emit:3``
#: cursor) and the connection — the statement-window kill pins' seam.
#: (``ConnLike`` spelled as ``Any`` at runtime — the alias is a
#: TYPE_CHECKING-only import; the hook's contract is the statement
#: boundary it fires at, not the connection's type.)
WindowHook = Callable[[str, Any], Awaitable[None]]


class EmitFencedError(RuntimeError):
    """The emit lost its exact claim or the run's durable terminal fence."""


class EmitCancelledError(EmitFencedError):
    """Cancellation won before this page's cursor CAS; rollback is whole-page."""


class EmitStaleClaimError(EmitFencedError):
    """The attempt's claim identity no longer owns the source row."""


#: The active bound comes from all five carriers (32767 all-smallint;
#: BIGINT_MAX all-bigint), never from a widened subset. This is independent of
#: jobs.deps_pending's unchanged smallint child-count bound.


class PageDivergedError(RuntimeError):
    """The emit's children INSERT
    hit the idempotency arbiter — the
    page's rows ALREADY EXIST under the page's ``(flow, emit,
    map_index, step)`` keys — while the source's cursor checkpoint says
    the page never ran. The cursor and the row history have diverged:
    the atomic emit tx can produce this only from a cursor the row
    history outgrew (a hand-rewound checkpoint, a resumed source whose
    body re-pages an already-committed page). the RAW DEATH IT FIXES:
    the raw ``UniqueViolationError`` escaped the tx, the body boundary
    laddered it as transient, and the re-claim re-read the same cursor
    and collided again — the ladder burned to a raw-typed terminal,
    never named. the NAMED DEATH: this error terminalizes the source
    LOUDLY (the deterministic route — re-running cannot fix a diverged
    cursor), and the message carries the remedy: RE-SYNC the CURSOR (the
    checkpoint is the source row's metadata under
    :data:`EMIT_CURSOR_KEY` — point it at the last page the rows prove
    committed). the TX ROLLED BACK: the divergent page wrote NOTHING."""

    def __init__(self, flow_id: JobId, source_id: JobId, detail: str) -> None:
        super().__init__(
            f"the emit's page already exists (the idempotency arbiter "
            f"refused the re-insert): flow {flow_id}, source {source_id} — "
            f"the resume's cursor diverged from the row history. "
            f"THE REMEDY: re-sync the cursor (the source row's metadata "
            f"under '{EMIT_CURSOR_KEY}') to the last page the rows prove "
            f"committed; re-running without the re-sync collides again. "
            f"{detail}"
        )


class MapIndexExhaustedError(RuntimeError):
    """A page carried a record whose non-null identity is invalid.

    The identity must be an exact non-negative ``int`` within the
    schema-derived upper bound (32767 on the all-smallint schema;
    BIGINT_MAX = 2**63-1 = 9223372036854775807 after all five carriers widen;
    a high candidate checks the actual schema once). Bool, float and string
    are refused without coercion: coercing one would format a different
    idempotency key or reach the SQL codec mid-transaction. the BATCH
    SEMANTICS: a poison record kills ITSELF loudly, never ITS MATES — the
    emit emits the VALID page-mates first (their tx commits; the valid work
    survives), then raises THIS naming the offending identity. An all-poison
    page writes nothing. The deterministic route terminalizes the source with
    the bound on the record."""


EMIT_REFUSAL_STATUS_SQL_TEMPLATE = """\
SELECT j.status, j.metadata, j.cancel_phase, root.status AS root_status
FROM {schema}.jobs AS j
LEFT JOIN {schema}.jobs AS root
  ON root.id = (j.metadata->>'flow_id')::uuid
WHERE j.id = $1
"""


class EmitBackpressure(Exception):  # noqa: N818  # Why: mandated typed control-signal vocabulary, not an error ladder.
    """Ordinary capacity control: this whole page did not fit.

    No child, edge, or cursor write has occurred when this escapes. Only the
    owning source runner may translate it into the dedicated park law; it is
    deliberately outside ``Snooze``/``RetryAfter`` and the failure ladder.
    ``backpressure_timeout_s`` at the public emit boundary is diagnostic-only
    compatibility input, never a failure deadline for this control.
    """

    def __init__(self, *, page_width: int, bound: int, outstanding: int) -> None:
        self.page_width = page_width
        self.bound = bound
        self.outstanding = outstanding
        super().__init__(
            f"emit page width {page_width} plus {outstanding} outstanding rows "
            f"exceeds max_in_flight={bound}; the source parks without spending "
            "a retry until a committed terminal makes the whole page fit"
        )


@dataclass(frozen=True, slots=True)
class EmitWakeResult:
    """One flow-target drain invocation's committed outcome."""

    promoted: int
    intent_completed: bool
    missed: bool = False


class EmitReservationExceededError(Exception):
    """A reserved replay presented a wider page than its reservation granted.

    The excess was never granted: no child, edge, or cursor write has
    occurred, and the reserved source is terminalized through the named
    deterministic grant-violation route with its reservation removed. This
    is not another backpressure park (the reservation's grant is spent
    fencing the source, not waiting on capacity), and it is never a
    :class:`PageDivergedError` — the idempotency arbiter did not refuse; a
    width comparison is not an arbiter collision and never asks an operator
    to re-sync a cursor.
    """

    def __init__(self, *, flow_id: JobId, source_id: JobId, granted: int, presented: int) -> None:
        self.flow_id = flow_id
        self.source_id = source_id
        self.granted = granted
        self.presented = presented
        super().__init__(
            f"emit replay presented {presented} records past the reserved "
            f"page width {granted}: flow {flow_id}, source {source_id} — the "
            "excess was never granted; nothing was written and the reserved "
            "source is fenced (the deterministic grant-violation route)"
        )


class EmitCapacityCorruptionError(RuntimeError):
    """A bounded run's durable capacity facts are missing or malformed.

    Fail-closed: the observation/intent machinery refuses rather than
    silently treating the run as unbounded (a drifted bound record or a
    missing stamp is corruption, never an ordinary empty result)."""


@dataclass(frozen=True, slots=True)
class EmitCapacityObservation:
    """One derived-capacity observation's returned values (§2.3).

    ``charge = live_rows + reservations`` is computed from the returned
    values — never a persisted column. ``min_waiting_width`` is the minimum
    validated parked page width; zero is a valid width, so waiter presence
    is the NULL test, never a truthiness test."""

    live_rows: int
    reservations: int
    min_waiting_width: int | None

    @property
    def charge(self) -> int:
        return self.live_rows + self.reservations

    @property
    def waiter_present(self) -> bool:
        return self.min_waiting_width is not None

    def something_fits(self, bound: int) -> bool:
        """The single admission rule's threshold face:
        ``charge + min_waiting_width <= bound``."""
        return self.waiter_present and self.charge + int(self.min_waiting_width or 0) <= bound


async def observe_emit_capacity(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    source_id: JobId | None = None,
) -> EmitCapacityObservation:
    """The shared derived observation (§2.3): one hoisted status-leg scan of
    the flow's live non-root rows plus exact parks.

    ``source_id`` is the currently running source's own id for unreserved
    admission (the single authoritative own-row exclusion) and ``None`` for
    terminal, park, and drain observations — the park observation includes
    its own newly written row."""
    row = await conn.fetchrow(wsql.emit_capacity_observation, flow_id, source_id)
    if row is None:  # pragma: no cover - the aggregate always returns one row
        raise EmitCapacityCorruptionError(
            f"the derived capacity observation for flow {flow_id} returned no row"
        )
    live_rows = row["live_rows"]
    reservations = row["reservations"]
    min_waiting_width = row["min_waiting_width"]
    if (
        type(live_rows) is not int
        or type(reservations) is not int
        or live_rows < 0
        or reservations < 0
    ):
        raise EmitCapacityCorruptionError(
            f"the derived capacity observation for flow {flow_id} returned "
            f"non-integer or negative facts ({live_rows!r}, {reservations!r})"
        )
    if min_waiting_width is not None and (
        type(min_waiting_width) is not int or min_waiting_width < 0
    ):
        raise EmitCapacityCorruptionError(
            f"the derived capacity observation for flow {flow_id} returned a "
            f"malformed min_waiting_width ({min_waiting_width!r})"
        )
    return EmitCapacityObservation(
        live_rows=live_rows,
        reservations=reservations,
        min_waiting_width=min_waiting_width,
    )


def _intent_generation(row: asyncpg.Record) -> tuple[int, int, int]:
    """The locked intent row's validated facts (bound, wake, handled). Every
    value must be an exact in-domain integer; malformed facts fail closed."""
    if row is None:  # pyright: ignore[reportUnnecessaryComparison]  # Why: the runtime guard behind the typed door — the caller checked, but the census re-refuses here.
        raise EmitCapacityCorruptionError("the capacity intent row is missing")
    try:
        bound = row["bound"]
        wake = row["wake_generation"]
        handled = row["handled_generation"]
    except (KeyError, IndexError) as exc:
        raise EmitCapacityCorruptionError(
            f"the capacity intent row is missing a generation column: {exc}"
        ) from exc
    for name, value, minimum in (
        ("bound", bound, 1),
        ("wake_generation", wake, 0),
        ("handled_generation", handled, 0),
    ):
        if type(value) is not int or value < minimum:
            raise EmitCapacityCorruptionError(
                f"the capacity intent row's {name} is malformed: {value!r}"
            )
    if wake < handled:
        raise EmitCapacityCorruptionError(
            f"the capacity intent row's generations are contradictory "
            f"(wake {wake} < handled {handled})"
        )
    return int(bound), int(wake), int(handled)


async def ensure_emit_capacity(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    bound: int,
) -> int:
    """Stamp the bounded run's immutable capacity record once (create-time
    stamp or first new-binary bounded claim adoption) and return the
    authoritative stored bound. A record that already exists with a
    different bound fails closed — the bound is immutable, so a mismatch is
    corruption, never an update. The §2.1 whole-object-set preflight runs
    first (cached per schema): a schema lacking any part of the folded
    ``42_01`` object set refuses before any write."""
    if type(bound) is not int or bound < 1:
        raise ValueError(f"the emit bound must be a positive int; got {bound!r}")
    from taskq.workflows._capability import verify_emit_capacity_objects

    await verify_emit_capacity_objects(conn, wsql.schema)
    await conn.execute(wsql.emit_intent_stamp, flow_id, bound)
    row = await conn.fetchrow(wsql.emit_intent_bound, flow_id)
    if row is None or row["bound"] is None:
        raise EmitCapacityCorruptionError(
            f"the bounded run {flow_id} has no capacity record after its stamp"
        )
    stored = row["bound"]
    if type(stored) is not int or stored != bound:
        raise EmitCapacityCorruptionError(
            f"the bounded run {flow_id}'s stored bound {stored!r} contradicts "
            f"the compiled declaration's {bound!r} — the bound is immutable, "
            "so this is corruption"
        )
    return int(stored)


async def emit_capacity_event(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    probe: bool,
    missing_ok: bool = False,
    census_family: str = "lane",
) -> bool:
    """The §4.1 terminal event rule, run in the caller's transaction
    under the canonical root -> intent -> winning-nodes order.

    ``probe=True`` (a terminal event) executes the one indexed exact-marker
    existence probe first: a zero-row probe performs no count, no width
    query, no candidate scan, no notification, and no promotion.
    ``probe=False`` is the park event's own shape — the park seat does not
    call THIS function (its phases are split: the park adopts and locks
    the intent BEFORE its write and observes after, see
    :func:`adopt_and_lock_emit_intent` + :func:`emit_capacity_event_locked`);
    this function retains the combined form for the terminal/sweep seats
    and keeps the exact same observation and generation authority.

    Returns the one-bit ``intent_created`` result: True only when this event
    crossed the threshold (a waiter newly fits, no intent was pending) and
    created the durable generation bump with its accelerator notification.
    """
    from taskq.workflows._capability import verify_emit_capacity_objects

    await verify_emit_capacity_objects(conn, wsql.schema)
    if probe:
        if census_family == "maintenance":
            bump_emit_census("maintenance_probes")
        else:
            bump_emit_census("terminals")
            bump_emit_census("probes")
        if await conn.fetchval(wsql.emit_park_exists, flow_id) is None:
            return False
    intent = await conn.fetchrow(wsql.emit_intent_lock, flow_id)
    if intent is None:
        if missing_ok:
            # The sweep's estate-wide face: a flow with no capacity record
            # is an unbounded run (neither stamp nor intent row, no feature
            # tax) — nothing to observe and nothing to notify.
            return False
        raise EmitCapacityCorruptionError(
            f"the bounded run {flow_id} has no capacity record at its "
            "terminal/park event — the bound stamp is missing (fail-closed)"
        )
    return await emit_capacity_event_locked(
        conn,
        wsql,
        flow_id=flow_id,
        probe=probe,
        census_family=census_family,
        intent=_intent_generation(intent),
    )


async def adopt_and_lock_emit_intent(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    bound: int,
) -> tuple[int, int, int]:
    """The park seat's ADOPT/LOCK phase (A9.2): the §2.1 whole-object-set
    preflight, the bound record's one-time adoption, then the intent row's
    own ``FOR UPDATE`` — the park UPDATE's preceding serialization point.
    Returns the locked row's validated ``(bound, wake, handled)`` facts for
    the caller's observe/decide phase. One intent authority: the adoption
    and the lock are this module's existing primitive split across two
    calls, never a second stamp or a second generation owner."""
    from taskq.workflows._capability import verify_emit_capacity_objects

    await verify_emit_capacity_objects(conn, wsql.schema)
    await ensure_emit_capacity(conn, wsql, flow_id=flow_id, bound=bound)
    intent = await conn.fetchrow(wsql.emit_intent_lock, flow_id)
    if intent is None:
        raise EmitCapacityCorruptionError(
            f"the bounded run {flow_id} has no capacity record at its park "
            "seat's adopt/lock phase — the bound stamp is missing (fail-closed)"
        )
    return _intent_generation(intent)


async def emit_capacity_event_locked(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    probe: bool,
    census_family: str,
    intent: tuple[int, int, int],
) -> bool:
    """The OBSERVE/DECIDE phase against the caller's already-locked intent
    row (A9.2's park phase split): the census's own bump point, the one
    shared observation, and the threshold's generation decision. The park
    seat runs this AFTER its park write, so the observation includes the
    newly parked row (its own row is in the observation and excluded from
    the live charge). Exactly one census bump point and one generation
    decision per committed park/terminal event — never duplicated across
    the split phases."""
    bound, wake_generation, handled_generation = intent
    if not probe:
        # The park's own event (§4.1 rule 4): the park IS the K family's
        # admission decision and its one observation.
        bump_emit_census("parks")
        bump_emit_census("admission_decisions")
    if census_family == "maintenance":
        bump_emit_census("maintenance_observations")
    else:
        bump_emit_census("observations")
    observation = await observe_emit_capacity(conn, wsql, flow_id=flow_id)
    if not observation.something_fits(bound):
        return False
    if wake_generation != handled_generation:
        # An intent is already pending: no generation, no notification — the
        # durable fallback makes that suppression safe.
        return False
    created = await conn.fetchval(wsql.emit_intent_generation, flow_id)
    if census_family == "maintenance":
        bump_emit_census("maintenance_repairs")
    else:
        bump_emit_census("new_intents")
    if created is None:
        raise EmitCapacityCorruptionError(
            f"the bounded run {flow_id}'s wake generation refused to advance "
            "at its overflow bound (fail-closed corruption refusal)"
        )
    await conn.execute(
        "SELECT pg_notify($1, $2)",
        wf_emit_capacity_channel(wsql.schema),
        str(flow_id),
    )
    return True


class _AdmissionLostError(Exception):
    """The locked recheck lost capacity; roll back before typed parking."""


def _metadata_document(raw: object) -> dict[str, object]:
    decoded: object = _json_loads(raw) if isinstance(raw, str) else raw
    if not isinstance(decoded, dict):
        raise ValueError("a live emit-backpressure row must have object metadata")
    return cast("dict[str, object]", decoded)


async def _reserved_width(metadata: object) -> int | None:
    """The claimed source row's validated page-width reservation (§2.5), or
    None when the row carries no reservation key. Malformed facts fail
    closed — a reservation is exactly the two-key object
    ``{"version":1,"width":W}`` with a validated positive-or-zero integer
    width; None, bool, float, negative, a different version, and unknown
    keys never coerce."""
    document = _metadata_document(metadata)
    if "wf_emit_reservation" not in document:
        return None
    raw_reservation = document["wf_emit_reservation"]
    if not isinstance(raw_reservation, dict):
        raise EmitCapacityCorruptionError("metadata.wf_emit_reservation must be an object")
    reservation = cast("dict[str, object]", raw_reservation)
    if set(reservation) != {"version", "width"}:
        raise EmitCapacityCorruptionError(
            "metadata.wf_emit_reservation must carry exactly version and width"
        )
    if type(reservation["version"]) is not int or reservation["version"] != 1:
        raise EmitCapacityCorruptionError("unsupported wf_emit_reservation version")
    width = reservation["width"]
    if type(width) is not int or width < 0:
        raise EmitCapacityCorruptionError(
            f"wf_emit_reservation.width must be an integer >= 0; got {width!r}"
        )
    return width


async def _emit_tx(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    source_id: JobId,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int | None,
    children: Sequence[EmitChild],
    cursor: dict[str, object],
    child_ids: list[JobId],
    max_attempts: int,
    retry_kind: str,
    _window_hook: WindowHook | None,
    run_priority: int | None = None,
) -> None:
    """The emit tx's body (the caller owns the transaction): the children
    INSERT, the edge rows, the cursor checkpoint — the certified fork
    shapes re-bound, chunked parallel arrays, ids minted app-side.

    The children INSERT is the page's
    only unique-violation surface (the
    idempotency arbiter — the composite ``(scope, key)`` index). The raw
    ``UniqueViolationError`` used to escape whole: the body boundary
    laddered it transient, the re-claim re-read the same cursor and
    collided again — the ladder burned to a raw-typed death. Here the
    arbiter's refusal is :class:`PageDivergedError` — the named, loud
    death carrying the remedy (re-sync the cursor) — and the tx rolls
    back WHOLE (the divergent page wrote nothing).

    The five-carrier capability preflight is the CALLER's, on the bounded
    paths only: the reserved replay calls ``verify_emit_capacity_objects``
    on its transaction connection, the unreserved admission runs it inside
    ``ensure_emit_capacity``, and the explicit unbounded
    (``max_in_flight=None``) path deliberately enters with no catalog
    read."""
    # Function-local, never module level (the module's own TYPE_CHECKING
    # import law — the runtime name is needed only at the except).
    import asyncpg

    for start in range(0, len(children), FORK_CHUNK):
        chunk = children[start : start + FORK_CHUNK]
        ids = child_ids[start : start + FORK_CHUNK]
        try:
            await conn.execute(
                wsql.emit_children,
                ids,
                [c.actor for c in chunk],
                [c.queue for c in chunk],
                [_jsonb(c.payload) for c in chunk],
                [c.max_attempts or max_attempts for c in chunk],
                [c.retry_kind or canonical_retry_kind(retry_kind) for c in chunk],
                [source_id] * len(chunk),
                [c.map_index for c in chunk],
                [c.step_key for c in chunk],
                [c.trace_id for c in chunk],
                [
                    _jsonb(
                        _row_metadata(
                            flow_id,
                            rate_limits=c.rate_limits,
                            reservations=c.reservations,
                            retry=c.retry,
                        )
                    )
                    for c in chunk
                ],
                # The scope is the fork's own (one scope per flow) — the
                # uniqueness pair is (scope, key), the key carries the
                # emit scope + the per-record index.
                [f"workflow:{flow_id}"] * len(chunk),
                [f"wf:{flow_id}:emit:{c.map_index}:{c.step_key}" for c in chunk],
                # one cohort per run (the flow_id), the fork's own law at the
                # page inserts.
                [str(flow_id)] * len(chunk),
                [run_priority if run_priority is not None else (c.priority or 0) for c in chunk],
                [c.start_to_close for c in chunk],
                *[[row_retry_curve(c.retry_policy)[column] for c in chunk] for column in range(4)],
            )
        except asyncpg.UniqueViolationError as exc:
            raise PageDivergedError(
                flow_id,
                source_id,
                f"the colliding chunk carried map_indexes "
                f"{[c.map_index for c in chunk]} (step keys "
                f"{sorted({c.step_key for c in chunk})}); the tx rolled "
                "back whole — nothing was written",
            ) from exc
        if _window_hook is not None:
            await _window_hook("emit:1", conn)
        await conn.execute(
            wsql.emit_edges,
            ids,
            [source_id] * len(chunk),
            [flow_id] * len(chunk),
            # fail_closed: the edges reconcile emit debt (the fork's
            # own edges' role), and a mid-stream source death is the
            # honest fail-closed parent.
            ["fail_closed"] * len(chunk),
        )
        if _window_hook is not None:
            await _window_hook("emit:2", conn)

    # the cursor checkpoint — under the full claim fence. The metadata merge is
    # a top-level concat under one key: the caller's cursor document is never
    # diffed field-by-field, and the row's other metadata (flow_id, the hold
    # stamps) survives.
    fenced = await conn.fetchval(
        wsql.emit_cursor,
        source_id,
        _jsonb({EMIT_CURSOR_KEY: cursor}),
        worker_id,
        attempt,
        claim_epoch,
    )
    if _window_hook is not None:
        await _window_hook("emit:3", conn)
    if fenced is None:
        # Read the durable discriminator only after the atomic fence refused;
        # the reader performs no writes and cannot turn stale into cancelled.
        from taskq.workflows.api._sql_runner import render_sql

        observed = await conn.fetchrow(
            render_sql(EMIT_REFUSAL_STATUS_SQL_TEMPLATE, wsql.schema),
            source_id,
        )
        terminal = {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
        raw_metadata: object = observed["metadata"] if observed is not None else {}
        metadata = _json_loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
        cancelled = observed is not None and (
            observed["root_status"] in terminal
            or observed["cancel_phase"] != 0
            or (isinstance(metadata, dict) and "cancel_phase" in metadata)
        )
        if cancelled:
            raise EmitCancelledError(
                f"workflow cancellation fenced source {source_id}'s page; "
                "the children, edges and cursor rolled back atomically"
            )
        raise EmitStaleClaimError(
            f"the emit's cursor checkpoint lost this exact source claim "
            f"({source_id}, worker, attempt, claim_epoch); rollback wrote nothing"
        )


def _poison_detail(poison: Sequence[EmitChild], bound: int) -> str:
    """Name each invalid-identity class once (the direct values, never
    dataclass/payload equality)."""
    bad_types = [c.map_index for c in poison if type(c.map_index) is not int]
    negative = [c.map_index for c in poison if type(c.map_index) is int and c.map_index < 0]
    over = [c.map_index for c in poison if type(c.map_index) is int and c.map_index > bound]
    parts: list[str] = []
    if bad_types:
        parts.append(
            f"non-integer identities {bad_types!r} (the door requires an "
            "exact int; bool, float and string are refused, never coerced)"
        )
    if negative:
        parts.append(f"negative identities {negative!r}")
    if over:
        parts.append(f"identities past the supported upper bound ({bound}) {over!r}")
    return "; ".join(parts)


def _raise_poison(flow_id: JobId, source_id: JobId, poison: list[EmitChild], *, bound: int) -> None:
    """Name the page's poison records and raise (the ceiling's loud
    terminal — after the valid page-mates' tx committed, so the valid
    work survives on the record; the batch semantics, the error's
    docstring)."""
    raise MapIndexExhaustedError(
        f"the page carries invalid map_index identity: flow {flow_id}, "
        f"source {source_id}, the offending records' step keys "
        f"{sorted({c.step_key for c in poison})}, "
        f"{_poison_detail(poison, bound)} — the VALID page-mates COMMITTED "
        "above (a poison record kills itself loudly, never its mates); the "
        f"page's effective identity upper bound is {bound} (a high candidate "
        "resolves it from the five live carriers). On an all-smallint schema "
        "an explicit retry is valid only after the operator-gated POST; above "
        "BIGINT_MAX the exhaustion remains deterministic"
    )


async def emit_batch(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    source_id: JobId,
    worker_id: JobId,
    attempt: int,
    claim_epoch: int | None,
    children: Sequence[EmitChild],
    cursor: dict[str, object],
    max_attempts: int = 3,
    retry_kind: str = "transient",
    max_in_flight: int | None = None,
    backpressure_timeout_s: float = 30.0,
    _window_hook: WindowHook | None = None,
    run_priority: int | None = None,
) -> tuple[JobId, ...]:
    """Emit one page of chain starts: the children + the edges + the
    cursor checkpoint, one transaction, while the source stays
    ``running``.

    The REFUTED-CLAIM DISCIPLINE is enforced at the door: every child
    carries its ``map_index`` (the discriminator the fork's idempotency
    key and the ledger's arbiter both ride — a ``None`` is refused before
    any row exists) and its ``trace_id`` (the per-record lineage).

    The idempotency keys are PARENT-SCOPED at the emit scope —
    ``wf:{flow}:emit:{map_index}:{step_key}`` — distinct from the
    fork-at-finalize keys (``wf:{flow}:{parent_step}:{step}:{idx}``): a
    source that also forks at its finalize can never key-collide with its
    own emitted chains.

    *max_in_flight* is the workflow's declared whole-page bound (``None`` is
    explicit unbounded). Saturation raises :class:`EmitBackpressure` before
    any write; the source runner then releases its physical claim and parks
    on the exact marker with the park instant. The locked second gate remains
    the actual bound defense against concurrent emitters. The legacy timeout
    argument is retained only as diagnostic-age input and never turns healthy
    capacity into failure.

    A reserved replay (the row was promoted with a page-width reservation)
    is governed by its grant instead of the shared bound: a smaller or equal
    page commits and converts the reservation; a wider page raises
    :class:`EmitReservationExceededError` — the excess was never granted.
    Both bounded paths — the reserved replay and the unreserved admission —
    run the five-carrier capability preflight before any write; the explicit
    unbounded (``max_in_flight=None``) path deliberately pays no catalog read
    for a low page.

    Returns the minted child ids (uuid7 via the seam). Raises
    :class:`EmitFencedError` when the claim was superseded (nothing
    written). Raises :class:`EmitBackpressure` when the whole page cannot fit
    (nothing written and no elapsed-time failure). Raises
    :class:`EmitReservationExceededError` when a reserved replay presented a
    wider page than its reservation (nothing written). Raises
    :class:`PageDivergedError` when the page's rows already exist (the
    cursor diverged from the row history — the tx rolled back, nothing
    written). Raises :class:`MapIndexExhaustedError` when a record's
    ``map_index`` is invalid or past the schema-resolved upper bound — after
    emitting the valid page-mates (the batch semantics: a poison record kills
    ITSELF loudly, never its mates — the valid work SURVIVES on the record).
    Raises :class:`taskq.workflows._types.MapIndexSchemaError` before writing
    anything when a high candidate meets a missing, mixed or unsupported
    carrier shape. Raises :class:`WorkflowWorkerCapabilityError` before any
    write on a bounded LOW page — reserved replay or unreserved admission —
    against a missing, mixed or botched carrier shape (a high candidate's
    authoring-door resolve refuses a missing or mixed shape as
    :class:`MapIndexSchemaError` first)."""
    for child in children:
        if child.map_index is None:  # pyright: ignore[reportUnnecessaryComparison]  # Why: the runtime guard behind the typed door — EmitChild.map_index's int type is the declaration; a hand-built child carrying None must be refused here (the refuted-claim discipline), never trusted to the checker.
            raise ValueError(
                f"emit child {child.step_key!r} has map_index=None — the "
                "emit stamps map_index per record (the discriminator the "
                "fork's idempotency key and the ledger's arbiter both "
                "ride); without it every record's children collide onto "
                "one row (the measured 198 UniqueViolations)"
            )
        if not child.trace_id:
            raise ValueError(
                f"emit child {child.step_key!r} (map_index="
                f"{child.map_index}) has no trace_id — the emit stamps "
                "the per-record trace (the one-query lineage)"
            )

    # The identity is an exact, non-negative int. A high candidate probes the
    # ACTUAL five-carrier schema once (not once per row, and never for an
    # ordinary in-range page). All-smallint preserves the poison/mate rule;
    # all-bigint activates immediately, with no restart or cached permission.
    # Missing, unknown or mixed carriers refuse the whole batch before writing.
    # The partition compares direct values — never dataclass/payload equality.
    needs_schema_probe = any(
        type(c.map_index) is int and c.map_index > MAP_INDEX_CEILING for c in children
    )
    bound = MAP_INDEX_CEILING
    if needs_schema_probe:
        async with pool.acquire() as probe_conn:
            bound = await resolve_map_index_domain(probe_conn, wsql.schema)

    valid: list[EmitChild] = []
    poison: list[EmitChild] = []
    for child in children:
        value = child.map_index
        if type(value) is not int or value < 0 or value > bound:
            poison.append(child)
        else:
            valid.append(child)
    if not valid and poison:
        raise MapIndexExhaustedError(
            f"the emit's page carries invalid map_index identity: flow "
            f"{flow_id}, source {source_id}, the offending records' step keys "
            f"{sorted({c.step_key for c in poison})}, "
            f"{_poison_detail(poison, bound)} — nothing was written (the "
            f"record's identity cannot ride the ledger's arbiter; the page's "
            f"effective identity upper bound is {bound})"
        )

    child_ids = [JobId(new_uuid()) for _ in valid]
    # Compatibility only: wait age belongs to observability, not a provider or
    # source retry deadline. Deliberately no clock/poll loop survives here.
    _ = backpressure_timeout_s

    if max_in_flight is not None:
        # an admission that can never succeed is refused at the door.
        if len(valid) > max_in_flight:
            raise ValueError(
                f"the emit's page ({len(valid)} children) can never fit "
                f"under the declared max_in_flight={max_in_flight} — the "
                "admission is impossible (shrink the page or raise the "
                "bound; the loud config refusal, never a silent starve)"
            )

        # The reserved replay's own law (§2.5): the source was promoted with
        # a page-width reservation. A smaller or equal page is charge-neutral
        # or freeing and commits without a new fit test; a wider page was
        # never granted.
        async with pool.acquire() as conn:
            source_metadata = await conn.fetchval(
                "SELECT metadata FROM {schema}.jobs WHERE id = $1".replace("{schema}", wsql.schema),
                source_id,
            )
        granted = await _reserved_width(source_metadata)
        if granted is not None:
            if len(valid) > granted:
                raise EmitReservationExceededError(
                    flow_id=flow_id,
                    source_id=source_id,
                    granted=granted,
                    presented=len(valid),
                )
            async with pool.acquire() as conn, conn.transaction():
                from taskq.workflows._capability import verify_emit_capacity_objects

                await verify_emit_capacity_objects(conn, wsql.schema)
                await _emit_tx(
                    conn,
                    wsql,
                    flow_id=flow_id,
                    source_id=source_id,
                    worker_id=worker_id,
                    attempt=attempt,
                    claim_epoch=claim_epoch,
                    children=valid,
                    cursor=cursor,
                    child_ids=child_ids,
                    max_attempts=max_attempts,
                    retry_kind=retry_kind,
                    _window_hook=_window_hook,
                    run_priority=run_priority,
                )
            bump_emit_census("admission_decisions")
            if poison:
                _raise_poison(flow_id, source_id, poison, bound=bound)
            return tuple(child_ids)

        # The single authoritative unreserved admission: `$2 = source_id` plus
        # `charge + W <= bound` over the shared derived observation. There is
        # no own-row subtraction and no second comparison.
        async with pool.acquire() as conn:
            observation = await observe_emit_capacity(
                conn, wsql, flow_id=flow_id, source_id=source_id
            )
        charge = observation.charge
        if charge + len(valid) > max_in_flight:
            raise EmitBackpressure(page_width=len(valid), bound=max_in_flight, outstanding=charge)
        inside_charge = charge
        try:
            async with pool.acquire() as conn, conn.transaction():
                await conn.execute(wsql.emit_admission_lock, str(flow_id))
                # The bounded run's one-time adoption: existing active runs
                # get their immutable bound record stamped at the first
                # new-binary bounded claim (a no-op probe once stamped).
                # THE STORED BOUND IS THE ADMISSION'S AUTHORITY (§2.1): the
                # stamp reads the stored bound back and fails closed on a
                # mismatch with the compiled declaration (a declaration
                # change / deploy skew never widens a live run's bound) —
                # the locked recheck compares against the STORED bound.
                stored_bound = await ensure_emit_capacity(
                    conn, wsql, flow_id=flow_id, bound=max_in_flight
                )
                inside = await observe_emit_capacity(
                    conn, wsql, flow_id=flow_id, source_id=source_id
                )
                inside_charge = inside.charge
                if inside_charge + len(valid) > stored_bound:
                    raise _AdmissionLostError
                await _emit_tx(
                    conn,
                    wsql,
                    flow_id=flow_id,
                    source_id=source_id,
                    worker_id=worker_id,
                    attempt=attempt,
                    claim_epoch=claim_epoch,
                    children=valid,
                    cursor=cursor,
                    child_ids=child_ids,
                    max_attempts=max_attempts,
                    retry_kind=retry_kind,
                    _window_hook=_window_hook,
                    run_priority=run_priority,
                )
            bump_emit_census("admission_decisions")
            if poison:
                _raise_poison(flow_id, source_id, poison, bound=bound)
            return tuple(child_ids)
        except _AdmissionLostError:
            raise EmitBackpressure(
                page_width=len(valid), bound=max_in_flight, outstanding=inside_charge
            ) from None

    async with pool.acquire() as conn, conn.transaction():
        await _emit_tx(
            conn,
            wsql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker_id,
            attempt=attempt,
            claim_epoch=claim_epoch,
            children=valid,
            cursor=cursor,
            child_ids=child_ids,
            max_attempts=max_attempts,
            retry_kind=retry_kind,
            _window_hook=_window_hook,
            run_priority=run_priority,
        )
    # the poison's loud death is outside the tx (the valid work committed).
    if poison:
        _raise_poison(flow_id, source_id, poison, bound=bound)
    return tuple(child_ids)


#: The flow-target drain's hard committed-transaction ceiling (§6): at most
#: 200 winning source CASes and queue notifications in one transaction.
FLOW_DRAIN_MAX_WINS: Final[int] = 200


async def wake_emit_backpressure(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
) -> EmitWakeResult:
    """The one promotion primitive: a commit-acknowledged, run-targeted
    drain of this flow's parked waiters (§1, §3.1, §6).

    Canonical order per transaction: the run's intent/admission advisory
    lock, the LIVE root row (``step_key='__flow__'``) ``FOR UPDATE``, the
    durable intent row, then the winning-nodes CAS. The root lock
    serializes the drain with the root-failure and maintenance seats
    (which hold the same row lock): a root-finalize either runs before
    the drain — the §5.4 fence fences the still-parked source — or after
    it, where the promoted source is unresolved live work and BOTH root
    -failure seats (the maintenance's ``has_unresolved`` aggregate and
    the failure cascade's own unresolved-live gate) legitimately withhold
    the finalize while that work rests — the run continues and the later
    pass owns the terminal once the live work rests. A terminal root can
    never be raced by a promotion that then strands a reserved pending
    source under it.
    Each iteration runs one fresh shared observation (the derived
    ``charge``) and the single authoritative candidate comparison —
    the oldest FITTING waiter on real rows, never a cached head. A
    winning CAS attaches the durable page-width reservation, removes the
    live marker atomically, and emits the effective queue wake. At most
    :data:`FLOW_DRAIN_MAX_WINS` winning CASes/notifications commit in one
    transaction; at that ceiling the SAME transaction performs one final
    shared observation and fitting-candidate comparison — if no candidate
    fits, that same transaction marks the intent handled, commits, and
    terminates the invocation (no empty fresh transaction, no second lock
    acquisition); if another candidate fits, it performs no 201st CAS and
    does not mark handled — it commits its 200 wins and immediately opens
    the next fresh chunk transaction under the still-pending generation
    (progress is the necessary condition for continuation).

    Completion is exactly a fresh observation returning no fitting
    candidate — only then does the same transaction set
    ``handled_generation = wake_generation``. A zero-row CAS never marks the
    intent handled and never retries in a loop: the invocation commits any
    earlier winning promotions, leaves the generation pending, and ends. A
    durable intent left pending is the terminal/park events' and the bounded
    recovery arms' to observe; notifications are accelerators, never
    authority.
    """
    total = 0
    completed = False
    missed = False
    while True:
        wins_tx = 0
        async with pool.acquire() as conn, conn.transaction():
            # D IS PER ENTERED DRAIN TRANSACTION (A10): the census's first
            # statement inside the transaction, so pool-acquisition or
            # transaction-entry failure is not counted as an entered drain
            # transaction, and every entered chunk transaction — including
            # one that subsequently fails — is counted exactly once.
            bump_emit_census("flow_drains")
            from taskq.workflows._capability import verify_emit_capacity_objects

            await verify_emit_capacity_objects(conn, wsql.schema)
            await conn.execute(wsql.emit_admission_lock, str(flow_id))
            # TEST-ONLY FAULT-INJECTION POINT (the kill-window control's
            # mid-chunk seam, documented as test-only and never a
            # production default): when the env is set AND a previous
            # chunk of THIS invocation already committed its wins, the
            # drain PAUSES here — inside the next chunk's transaction,
            # holding the flow's ADMISSION lock (and ONLY it: every other
            # drain arm queues behind it, while concurrent terminals'
            # generation bumps proceed untouched) — after committed wins
            # and with the generation still pending, so a control can
            # SIGKILL this process in the exact mid-chunk state. The env
            # is read per call and never cached; unset (every production
            # process) the hook is absent and this is dead text.
            if total > 0:
                chunk_pause_s = os.environ.get("TASKQ_TEST_DRAIN_CHUNK_PAUSE_S")
                if chunk_pause_s:
                    await asyncio.sleep(float(chunk_pause_s))
            # THE LIVE ROOT LOCK (§3.1's canonical order): the root row
            # lock serializes this chunk with the root-failure and
            # maintenance seats — a root-finalize cannot interleave
            # between this transaction's observations and CASes, so a
            # promoted source can never be stranded pending-with-
            # reservation under a root that terminalized behind it.
            root_row = await conn.fetchval(wsql.emit_root_lock, flow_id)
            if root_row is None:
                # The flow's root row is absent: not a live bounded run's
                # drainable shape — nothing to drain and no intent to
                # complete (the run's own records cannot exist without
                # it). Fail closed: the caller's error path owns the
                # already-committed state.
                raise EmitCapacityCorruptionError(
                    f"the bounded run {flow_id} has no live root row at its "
                    "drain — the __flow__ row is missing (fail-closed)"
                )
            intent = await conn.fetchrow(wsql.emit_intent_lock, flow_id)
            if intent is None:
                raise EmitCapacityCorruptionError(
                    f"the bounded run {flow_id} has no capacity record at its "
                    "drain — the bound stamp is missing (fail-closed)"
                )
            bound, _wake_generation, _handled_generation = _intent_generation(intent)
            while True:
                # The drain's reads are the §4.2 V-family's own units in
                # ACTUAL SQL QUERY EXECUTIONS (A10): each read point is the
                # shared aggregate observation plus the fitting-candidate
                # query (bumped by 2) — never duplicated into the event-side
                # E census.
                observation = await observe_emit_capacity(conn, wsql, flow_id=flow_id)
                bump_emit_census("candidate_reads", 2)
                candidate = await conn.fetchrow(
                    wsql.emit_candidate, flow_id, observation.charge, bound
                )
                if candidate is None:
                    # Completion: the fresh observation returned no fitting
                    # candidate (including the case with no waiters) — THIS
                    # transaction marks the intent handled (the ceiling
                    # rule's final observation belongs to the current full
                    # transaction; no empty fresh transaction follows).
                    await conn.fetchval(wsql.emit_intent_handled, flow_id)
                    completed = True
                    break
                if wins_tx == FLOW_DRAIN_MAX_WINS:
                    # THE 200-WIN CEILING's final fresh observation returned
                    # ANOTHER fitting candidate: no 201st CAS and no handled
                    # mark — the intent stays pending, this transaction
                    # commits its 200 wins, and the next chunk transaction
                    # owns the grant (progress is the necessary condition
                    # for continuation).
                    break
                promoted = await conn.fetchrow(
                    wsql.emit_backpressure_wake, candidate["id"], int(candidate["width"])
                )
                if promoted is None:
                    # The exact real row changed between the candidate
                    # comparison and the CAS — an ordinary miss, not
                    # corruption: no handled mark, no immediate re-read.
                    missed = True
                    break
                wins_tx += 1
                total += 1
                bump_emit_census("fitted_grants")
                # NOTIFY accelerates an already durable pending transition.
                # The effective actor assignment, not a stale original label,
                # is named.
                await conn.execute(
                    "SELECT pg_notify($1, $2)",
                    wake_channel(wsql.schema),
                    str(promoted["effective_queue"]),
                )
        if missed or completed or wins_tx < FLOW_DRAIN_MAX_WINS:
            break
        # The chunk committed at its 200-win ceiling whose final in
        # -transaction observation returned another fitting candidate (the
        # final pair already ran in THIS transaction): the next chunk's
        # first fresh observation is an ordinary read point, never the
        # ceiling rule's final observation.
    return EmitWakeResult(promoted=total, intent_completed=completed, missed=missed)


#: The zero UUID — the rotation cursor's reset value (the wrap face).
_ZERO_UUID: Final[JobId] = JobId(UUID("00000000-0000-0000-0000-000000000000"))


async def drain_pending_emit_intents(
    pool: asyncpg.Pool,
    wsql: WorkflowSql,
    *,
    trigger: str = "notification",
) -> int:
    """The leader's global drain (§6): the pending-intent selection — at most
    100 distinct flow ids in one short indexed read, under the persistent
    rotating ``after_flow_id`` cursor — followed by each selected flow's own
    bounded drain transaction (each commits independently; no row lock is
    retained past the selection).

    A full selection persists its last ``flow_id`` as the rotation cursor
    even if a selected flow failed; a short selection resets the cursor to
    the zero UUID after its flows were offered, so the next selection wraps;
    an empty ordered read wraps immediately once. For a fixed set of N
    continuously pending flows every flow is offered at least once within
    ``ceil(N/100) + 1`` selections, independent of the failing prefix."""
    if trigger == "notification":
        # The committed-notification/leadership/reconnect arm's own
        # invocation (the V family's U).
        bump_emit_census("global_drains")
    else:
        # The maintenance-interval selection (the M family).
        bump_emit_census("maintenance_selections")
    async with pool.acquire() as conn:
        row = await conn.fetchrow(wsql.emit_rotation_read)
        after: JobId = _ZERO_UUID
        if row is not None and row["after_flow_id"] is not None:
            after = JobId(row["after_flow_id"])
    offered = 0
    wrapped = False
    while True:
        async with pool.acquire() as conn:
            rows = await conn.fetch(wsql.emit_pending_intents, after)
        if not rows:
            if after != _ZERO_UUID and not wrapped:
                wrapped = True
                after = _ZERO_UUID
                continue
            break
        for intent_row in rows:
            flow = JobId(intent_row["flow_id"])
            try:
                await wake_emit_backpressure(pool, wsql, flow_id=flow)
            except Exception as exc:
                # One flow's failure never suppresses the rotation's
                # advancement (§6): the bounded diagnostic stands in for the
                # flow, later flows are still offered, and the failed flow's
                # own pending intent is retried on a later selection. THE
                # SIBLINGS' DIAGNOSTIC CONTRACT (M-4): the same
                # `error_class`/`error` fields both sibling seats log — an
                # EmitCapacityCorruptionError raised by the drain's own
                # typed doors is NAMED here (a per-flow refusal that
                # re-offers every selection with no name is an anonymous
                # warning the operator cannot alert on).
                logger.warning(
                    "wf.emit_intent_drain_flow_failed",
                    flow_id=str(flow),
                    offered_anyway=True,
                    error_class=type(exc).__name__,
                    error=str(exc)[:300],
                )
            offered += 1
        if len(rows) < WF_EMIT_ROTATION_BATCH:
            async with pool.acquire() as conn:
                await conn.execute(wsql.emit_rotation_write, _ZERO_UUID)
            break
        after = JobId(rows[-1]["flow_id"])
        # The rotation is committed independently of the per-flow work.
        async with pool.acquire() as conn:
            await conn.execute(wsql.emit_rotation_write, after)
    return offered


#: The marker sweep's per-process rotation cursor (D7's bounded keyset):
#: schema → the last visited flow id. Persistent for the leader process's
#: lifetime; a leader restart re-walks from the start (the walk completes
#: within ceil(N/100) + 1 selections and the repaired flows leave the
#: selection through their created intent rows). The intent selection's
#: own cursor is the durable §6 singleton; the sweep's is process-local
#: because the sweep's authority (the exact marker) re-derives its whole
#: work set from real rows every call — a restart loses only position,
#: never work.
_SWEEP_CURSORS: dict[str, str] = {}

#: The marker sweep's per-call flow batch — the ONE shared rotation bound
#: (L-3: the SQL ``LIMIT`` renders from `WF_EMIT_ROTATION_BATCH` and the
#: Python checks compare against it; a second spelling would let the SQL
#: LIMIT exceed the Python literal, every selection would look "short",
#: the rotation cursor would reset, and flows past the first batch would
#: never be offered).
_SWEEP_BATCH: Final[int] = WF_EMIT_ROTATION_BATCH


async def sweep_emit_markers(pool: asyncpg.Pool, wsql: WorkflowSql) -> int:
    """The bounded exact-marker recovery sweep (§5.3): visit only the
    ``jobs_wf_emit_park_idx`` partial index, derive just the marked bounded
    flows that still LACK their required intent record, and repair each in
    its OWN transaction — the one-time bound adoption from the marker's
    own validated facts, then the §4.1 threshold rule itself (the shared
    observation; a generation bump only when a waiter NEWLY fits and no
    intent is pending).

    The selection is an ordered KEYSET rotation (D7): ``flow_id >
    $after`` in ``ORDER BY flow_id`` order, at most 100 flows per call,
    the caller's process-local cursor persisting the last visited flow;
    an empty read wraps the cursor once. A full selection persists its
    last flow id; a short selection resets to the start. A repaired flow
    leaves the selection through its created intent row.

    The SELECTION's re-observation law (N3, the maintenance cadence): a
    handled-but-not-fitting marked flow (its intent exists with
    ``wake = handled``) is RE-OBSERVED at each maintenance interval —
    exactly one real observation per sweep, no re-bump of its
    generations — so the steady state is NOT an empty read for every
    flow: a flow whose parked waiter still cannot fit keeps its single
    per-sweep observation until the rows change. Only the handled/pending
    rules stated in the selection's own SQL may be claimed of it.

    Each flow's repair is its OWN transaction: a malformed marker bound
    or width poisons only its own flow — the flow is quarantined with a
    bounded diagnostic and retried on a later selection, and every other
    selected flow's repair commits. An empty index result ends the
    sweep's work: there is no parked-estate scan, no source re-claim, no
    due-time wake, and NO re-bump of an intent the drain already handled
    (that would be a busy loop, not recovery).

    THE LOCK-ORDER EXEMPTION (A9.2's named list, the real reason): this
    seat takes NO root lock. Its per-flow transaction reads the marker
    and bound without ``FOR UPDATE`` and adopts/locks only the flow's
    single intent row — it never queues a root holder behind work it
    already owns, so a root lock would be pure serialization with the
    drain/cascade seats and no cycle to prevent. Contended intent rows
    defer to a later selection."""
    after = _SWEEP_CURSORS.get(wsql.schema, str(_ZERO_UUID))
    async with pool.acquire() as conn:
        rows = await conn.fetch(wsql.emit_marker_sweep, after)
    if not rows:
        # An empty ordered read wraps the cursor immediately once (the
        # §6 rotation's own semantics): the walk restarts on the next
        # call, and this call performed zero per-flow work.
        _SWEEP_CURSORS[wsql.schema] = str(_ZERO_UUID)
        return 0
    repaired = 0
    for row in rows:
        flow = JobId(row["flow_id"])
        try:
            async with pool.acquire() as conn, conn.transaction():
                # The flow's own validated marker bound: a malformed
                # marker raises HERE — inside this flow's own repair
                # transaction — quarantining the poison to this flow.
                raw_bound = await conn.fetchval(wsql.emit_marker_flow_bound, flow)
                if raw_bound is None or type(raw_bound) is not int or raw_bound < 1:
                    raise EmitCapacityCorruptionError(
                        f"the exact-marker sweep found flow {flow} with a malformed "
                        f"stored bound ({raw_bound!r})"
                    )
                await ensure_emit_capacity(conn, wsql, flow_id=flow, bound=int(raw_bound))
                # The threshold rule, exactly as the terminal/park events
                # run it: derive once; a non-fitting waiter never re-arms
                # the intent the drain handled.
                if await emit_capacity_event(
                    conn, wsql, flow_id=flow, probe=True, census_family="maintenance"
                ):
                    repaired += 1
        except Exception as exc:
            # One flow's malformed marker never aborts the others'
            # repairs (D7's per-flow isolation): the bounded diagnostic
            # stands in for the flow — NAMING the fault (the operator's
            # own quarantine record) — and it is retried on a later
            # selection, never dropped.
            logger.warning(
                "wf.emit_marker_sweep_flow_failed",
                flow_id=str(flow),
                offered_anyway=True,
                error_class=type(exc).__name__,
                error=str(exc)[:300],
            )
    if len(rows) >= _SWEEP_BATCH:
        # A full selection persists its last flow id as the rotation
        # cursor even if a selected flow failed.
        _SWEEP_CURSORS[wsql.schema] = str(rows[-1]["flow_id"])
    else:
        # A short selection resets the cursor to the start after its
        # flows were offered (the §6 wrap shape).
        _SWEEP_CURSORS[wsql.schema] = str(_ZERO_UUID)
    if repaired:
        logger.warning(
            "wf.emit_marker_sweep_repaired",
            kind="wf_emit_marker_sweep_repaired",
            repaired=repaired,
        )
    return repaired


async def fence_not_draining_parked_sources(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    child_id: JobId | None,
    child_step_key: str | None = None,
) -> int:
    """The §5.4 loud refusal, run in the CALLER's failure-cascade
    transaction.

    The proof condition is precise and durable: the root terminal
    ``failed`` has just been committed behind a genuine child failure that
    exhausted its declared retry policy (the caller owns that proof — the
    cascade seat is this function's own caller; the maintenance seat is
    the migration's SQL function), while an exact parked source of the
    flow still holds un-emitted work — INCLUDING a first-page park with
    no committed ``emit_cursor`` (the absent key means zero pages have
    committed and the default/initial cursor identifies the first page;
    it does not make the park empty or abandonable). The fence
    terminalizes every such parked source with
    ``WORKFLOW_NOT_DRAINING_CLASS``, the named reason, and the causal
    child/root ids; it spends no source logical attempt, creates no ladder
    retry, and never overwrites the child's genuine error. Returns the
    fenced count for the caller's log/metric."""
    from taskq.constants import WORKFLOW_NOT_DRAINING_CLASS

    row = await conn.fetchrow(
        wsql.wf_not_draining_fence,
        flow_id,
        WORKFLOW_NOT_DRAINING_CLASS,
        (
            "the run is provably not draining: the root terminal 'failed' "
            "committed after the causal child failure exhausted its retry "
            "policy, while this source still holds un-emitted work "
            "(including the first page when no cursor has committed; "
            "reason: terminal_root_after_child_failure_exhaustion)"
        ),
        child_id,
    )
    fenced = int(row["fenced_count"]) if row is not None else 0
    if fenced:
        # THE ONE ATTRIBUTE CONTRACT (M-3's correction): both seats (this
        # cascade seat and the maintenance seat) emit exactly
        # ``error_class`` — the same constant — so an error_class
        # -filtered alert sees every refusal.
        NOT_DRAINING_COUNTER.add(fenced, {"error_class": WORKFLOW_NOT_DRAINING_CLASS})
        logger.warning(
            "wf.not_draining_refusal",
            flow_id=str(flow_id),
            child_id=str(child_id) if child_id else None,
            child_step_key=child_step_key,
            error_class=WORKFLOW_NOT_DRAINING_CLASS,
            reason="terminal_root_after_child_failure_exhaustion",
            fenced=fenced,
        )
    return fenced
