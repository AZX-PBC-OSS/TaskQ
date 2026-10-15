"""The workflow status derivation + the rows-only reconstruction (T08).

THE §17.5 DERIVATION TABLE — the workflow-level status derived from the
NODE statuses, precedence applied IN ORDER. No status cache exists to
corrupt: the ledger is the status store, and the derivation is a PURE
function over the grouped rollup's rows (the crash-safety proof is the
derivation's corollary — the status can be RECONSTRUCTED from rows alone;
there is no cache, only rows to re-read).

THE ABSORBED-FAILURE CLAUSE COMES FIRST (re-red-team B2 — the clause that
makes T06's gate and T07's maybe pin hold): a ``failed`` node whose
failure was ABSORBED by its edge's declared policy (``collect``, or a
``maybe`` edge — the edge ledger records the policy; the join row's
``failures`` array records the absorption) derives through its PARENT'S
outcome, NOT ``failed`` — a partial-failure collect (997 Ok + 3 Failed)
and a maybe-absorption derive ``complete``/the parent's outcome, never
``failed``. The absorption is on the record: the edge ledger's policy +
the absorbed-child list are how the derivation KNOWS — never a heuristic.

THE CANONICAL NODE VOCABULARY (the two derived pending-row shapes):
join-wait = ``pending`` + ``deps_pending > 0``; held = ``pending`` + the
``scheduled_at`` deadline + the unresolved signal row. No ``hold`` /
``pending_join`` status exists to group — both are pending-row
representations, derived to ``blocked`` here.

THE TERMINAL-CRASH CLASS (T20/T21's crashed-terminal wedge — the
semantics decision, the design law): a jobs row in status ``crashed`` or
``abandoned`` is TERMINAL-FOR-REAL, and the derivation folds it into the
FAILED-CLASS terminal. THE WHY: the state machine's totality table gives
both statuses ZERO outbound transitions
(``statemachine.VALID_TRANSITIONS``); a row reaches ``crashed`` only
through the reclaim's crashed branch — the attempt budget exhausted
(``{has_budget}`` false at reclaim time) — and ``abandoned`` only
through ``mark_abandoned`` (the rolling-deploy record). NOTHING will
ever revive either: the row's OWN state decides, deterministically. The
REAL crash recovery is the reclaim of a ``running`` row whose holder
died WITH budget remaining — that row re-pends and never carries the
crash terminal, so ``crashed`` is never the reclaim's input (the old
row-1 comment claimed it was, and the wedge was the proof: a
{succeeded, crashed} run derived ``running`` forever — G7 green on a
dead run, retention holding the corpse forever, because no sweep arm
touches a crashed row and the derivation refused to terminalize it).
The asymmetry doctrine: a wedge (the flow never terminal) is the state
an operator must notice FOREVER; an honest failed terminal is the
runbook's resumable-by-rerun. The LEDGER's ``crashed`` rows are a
DIFFERENT class: the attempt-level reclaim receipt (the row re-pends —
see ``reconstruct_workflow_status``), and they stay out of the terminal
read.

§17.5's no-zombie promise holds BY this fold: every node status maps to
exactly one workflow status, and the terminal classes cannot derive
liveness.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Final, Literal

from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows._sql import WorkflowSql

__all__ = [
    "NodeView",
    "WorkflowStatus",
    "derive_workflow_status",
    "reconstruct_workflow_status",
]

#: The workflow-level statuses the derivation yields (exactly one, per the
#: totality property). ``blocked`` covers held + join-wait + blocked rows.
WorkflowStatus = Literal["running", "failed", "blocked", "complete", "cancelled", "pending"]

#: The jobs-table terminal statuses (the statemachine's vocabulary) —
#: the reconstruction's never-granted read draws the terminal CLASSES
#: from this set (``_NEVER_GRANTED_SQL``), and the derivation folds the
#: terminal-crash class into the failed-class terminal.
_TERMINAL: Final[frozenset[str]] = frozenset(
    {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
)

#: THE TERMINAL-CRASH CLASS — the node statuses that fold into the
#: failed-class terminal (the module docstring's semantics decision):
#: both are deterministic deaths with zero outbound transitions in the
#: state machine's totality table, and no sweep arm ever reclaims them.
_TERMINAL_CRASH_CLASS: Final[frozenset[str]] = frozenset({"crashed", "abandoned"})


@dataclass(frozen=True, slots=True)
class NodeView:
    """One node's derived view — the per-node rollup row's inputs plus the
    pending-row shapes (T08's canonical vocabulary):

    * ``status`` — the jobs row's status;
    * ``deps_pending`` — the join counter (join-wait = pending + > 0);
    * ``blocking_reason`` — the row's metadata.blocking_reason (a
      blocked-with-reason row: orphan_parent / failed_parent /
      body_unavailable);
    * ``held`` — the held-row representation (T03: pending + the
      ``scheduled_at`` deadline + the unresolved signal row);
    * ``absorbed`` — the node's failure was ABSORBED by its edge's
      declared policy (collect / maybe — the edge ledger + the join row's
      ``failures`` array are the record; never a heuristic). A SUCCEEDED
      or non-failed node is never absorbed (the clause reads ``failed``
      nodes only).
    * ``cancel_in_flight`` — the run has a cancel in flight (the
      derivation's first row reads it beside ``running``).
    """

    status: str
    deps_pending: int = 0
    blocking_reason: str | None = None
    held: bool = False
    absorbed: bool = False
    cancel_in_flight: bool = False

    @property
    def is_join_wait(self) -> bool:
        """Join-wait: pending + deps_pending > 0 (T03's representation —
        the name T08/T15 reference)."""
        return self.status == "pending" and self.deps_pending > 0

    @property
    def is_blocked_row(self) -> bool:
        """A pending-row representation that resolves to ``blocked``:
        join-wait (deps > 0), or a blocked-with-reason stamp. Two markers
        never read as blocks: the bare 'join' marker on a deps == 0 row is
        a FIRED join (claimable like any other row), and 'body_unavailable'
        is the R2-2 loudness record on a fired join whose body never
        resolved — the delivery CONTINUED (the run proceeds; the operator
        sees the warning), the row is not blocked."""
        if self.status != "pending":
            return False
        if self.held:
            return True
        if self.deps_pending > 0:
            return True
        if self.blocking_reason is None:
            return False
        return self.blocking_reason not in ("join", "body_unavailable")

    @property
    def counts_failed(self) -> bool:
        """THE ABSORBED-FAILURE CLAUSE: a failed node whose failure was
        absorbed derives through its PARENT'S outcome — it does not
        trigger the derivation's ``failed`` row."""
        return self.status == "failed" and not self.absorbed


def derive_workflow_status(nodes: tuple[NodeView, ...]) -> WorkflowStatus:
    """The §17.5 derivation table, precedence applied in order. THE
    TOTALITY IS A PROPERTY (GAPS-ESTATE F6a): for EVERY input multiset the
    derivation yields EXACTLY ONE workflow status — a new node
    representation added without a table row reds the property (the
    totality is the test, ``tests/test_wf_status_property.py``).

    THE TERMINAL-CRASH FOLD comes first (the module docstring's semantics
    decision): ``crashed``/``abandoned`` node rows enter the table as the
    failed-class terminal — the raw status stays on the
    :class:`NodeView`, the fold is the derivation's own row-0."""
    folded = tuple(
        dataclasses.replace(n, status="failed") if n.status in _TERMINAL_CRASH_CLASS else n
        for n in nodes
    )
    nodes = folded
    # 1. any node running OR any cancel in flight → running. THE
    #    LIVENESS VOCABULARY (the crashed-terminal wedge's cure): exactly
    #    ONE jobs status is live-executing — ``running``, the reclaim
    #    arms' only input (a holder that died with budget remaining
    #    re-pends from here). ``crashed``/``abandoned`` are TERMINALS
    #    (zero outbound transitions; the reclaim's crashed branch wrote
    #    them precisely because nothing would ever revive the row) — the
    #    old predicate read them as "the reclaim's input" and the wedge
    #    was the proof: nothing ever reclaimed them, the corpse derived
    #    ``running`` forever.
    if any(n.status == "running" for n in nodes) or any(n.cancel_in_flight for n in nodes):
        return "running"
    # 2. THE ABSORBED FAILURES COME FIRST (B2): an absorbed failure derives
    #    through its parent's outcome — the failed row below reads only
    #    NON-absorbed failures (counts_failed).
    if any(n.counts_failed for n in nodes):
        return "failed"
    # 3. any node held, join-wait, or blocked → blocked (the pending-row
    #    representations — no hold/pending_join status exists to group).
    if any(n.is_blocked_row for n in nodes):
        return "blocked"
    # 4. all terminal-succeeded/skipped → complete. An ABSORBED failure
    #    derives through its parent here: the collect's workflow completes
    #    with the failure report (T06's gate; T07's maybe pin).
    if nodes and all(n.status in ("succeeded", "failed", "skipped") or n.absorbed for n in nodes):
        return "complete"
    # 5. any cancelled → cancelled.
    if any(n.status == "cancelled" for n in nodes):
        return "cancelled"
    # 6. nothing scheduled (plain queued nodes remain) → pending.
    return "pending"


# ── THE RECONSTRUCTION (D4's two-source rule) ───────────────────────────
#
# Reconstruction from rows alone covers TWO terminal classes:
#   * ATTEMPTED terminals via the LEDGER (the wf_step_ledger's terminal
#     rows — the attempt ran, the outcome is the ledger's);
#   * NEVER-GRANTED terminals (a budget-killed pending iteration, a
#     cancelled pending node, a skip) via the NODE ROW'S error jsonb —
#     they have NO ledger rows; the terminal CLASS rides the error row.
# The two-source rule is what makes the G7 always-on assertion lawful in
# every workflow integration test (the reconstruction helper the fixture
# calls; the docs state the rule).

_LEDGER_TERMINALS_SQL = """\
SELECT step_key, status FROM {schema}.wf_step_ledger
WHERE flow_id = $1::uuid AND status IN ('succeeded', 'failed')
"""

_NEVER_GRANTED_SQL = """\
SELECT step_key, status
FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1::uuid
  AND metadata ? 'flow_id'
  AND step_key <> '__flow__'
  AND status IN ('cancelled', 'crashed', 'abandoned')
"""


async def reconstruct_workflow_status(
    conn: ConnLike, wsql: WorkflowSql, flow_id: JobId
) -> WorkflowStatus:
    """The rows-only reconstruction (G7's always-on assertion's read): the
    reported workflow status must equal THIS — derived from the rows
    alone, no cache consulted. Two sources (D4): the ledger's attempted
    terminals + the node rows' error jsonb for the never-granted ones."""
    node_rows = await conn.fetch(wsql.workflow_nodes, flow_id)
    ledger_rows = await conn.fetch(_LEDGER_TERMINALS_SQL.format(schema=wsql.schema), flow_id)
    never_granted = await conn.fetch(_NEVER_GRANTED_SQL.format(schema=wsql.schema), flow_id)

    # The ABSORPTION record: which nodes' failures the edge ledger
    # absorbed (the policy + the absorbed-child list are ON THE RECORD).
    # The terminal-crash class reads through the SAME clause (the fold's
    # consistency: the derivation folds crashed/abandoned into the
    # failed-class terminal, so an absorbing edge absorbs the crash the
    # way it absorbs the failure — the SQL maintenance leg's
    # _absorbed_exists predicate is status-blind on the parent for
    # exactly this reason).
    absorbed_keys = {r["step_key"] for r in node_rows if r["absorbed"] and r["status"] in _TERMINAL}
    ledger_by_key: dict[str, str] = {}
    for r in ledger_rows:
        # The ledger's LATEST terminal row for the step (attempt-ordered
        # arrival — the dict overwrite is the latest-write truth).
        ledger_by_key[r["step_key"]] = r["status"]

    nodes: list[NodeView] = []
    for r in node_rows:
        status = r["status"]
        if status == "pending" and r["step_key"] in ledger_by_key:
            # The LEDGER IS THE STATUS STORE: a pending row whose ledger
            # terminal exists (the crash window) reconstructs through the
            # ledger's outcome.
            status = ledger_by_key[r["step_key"]]
        nodes.append(
            NodeView(
                status=status,
                deps_pending=r["deps_pending"],
                blocking_reason=r["blocking_reason"],
                absorbed=r["step_key"] in absorbed_keys,
            )
        )
    # The never-granted terminals (no ledger rows — the terminal CLASS
    # rides the node row's error jsonb / error columns): a cancelled
    # pending node, a budget-killed iteration, a skip. The
    # terminal-crash class rides here too — the row's own status IS the
    # record (the reclaim's crashed branch: the budget exhausted) — and
    # the derivation folds it into the failed-class terminal. THE
    # LEDGER'S 'crashed' ROWS STAY OUT of the terminal read above
    # (succeeded/failed only): a ledger crash is the ATTEMPT-level
    # reclaim receipt — the row re-pends and re-claims (the REAL crash
    # recovery), never a terminal the reconstruction would read through.
    for r in never_granted:
        nodes.append(NodeView(status=r["status"]))
    return derive_workflow_status(tuple(nodes))
