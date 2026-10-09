"""The workflows engine's named statement constants — the bundle
(GAPS-ESTATE F10).

Workflow SQL lives as named statement constants — the
``backend/_sql_templates.py`` / ``_dispatch_sql.py`` pattern, never inline
f-strings at call sites. The constants live in their concern's module
(``_sql_finalize`` / ``_sql_sweep`` / ``_sql_ledger``); THIS module is the
bundle: :meth:`WorkflowSql.build` renders every constant for one validated
schema (the schema identifier is the ONLY interpolated value — validated
via ``taskq.constants.require_schema``; every caller-controlled value uses
asyncpg ``$N`` positional parameter binding).

THE JSONB LANDMINE (the fanout proof's cut #1, HIGH — the rule's test
lives in the engine pins): asyncpg parses ``$N`` as a parameter placeholder
EVEN INSIDE a quoted JSONB literal. An f-string that renders
``'{"peer":$3}'::jsonb`` fails with ``invalid input syntax for type json``
on every attempt. The rule: parameterize all JSON (``$3::jsonb`` with a
dict param), NEVER interpolate into a JSONB literal. The literal jsonb
shapes here interpolate nothing.

Multi-row writes (fork fan-out, sweep fires, drain consumers) bind PARALLEL
ARRAYS through ``unnest`` — one statement per table per batch, never one
round trip per row (the 1000-child fan-out tx band's shape). Every ``id``
column is minted APP-SIDE through the ``taskq._ids`` seam (uuid7) and bound
as a parameter — DB-side and random-UUID generation are checker-banned
(TID251), and the seam-only generation pin greps this package.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from taskq.constants import require_schema
from taskq.workflows._sql_finalize import (
    COLLECT_FAN_IN_APPEND_SQL,
    COLLECT_FAN_IN_SQL,
    DECREMENT_ABSORBED_SQL,
    DECREMENT_SQL,
    EMIT_CHILDREN_SQL,
    EMIT_CURSOR_SQL,
    EMIT_EDGES_SQL,
    FAIL_CLOSED_CASCADE_SQL,
    FIRE_SQL,
    FLOW_STATUS_SQL,
    FORK_CHILDREN_SQL,
    FORK_CONSUMER_EDGES_SQL,
    FORK_EDGES_SQL,
    FORK_JOIN_CONSUMERS_SQL,
    FORK_JOIN_NODE_SQL,
    NODE_EDGE_SQL,
    NODE_INSERT_SQL,
    OUTBOX_INSERT_SQL,
    TERMINAL_MARK_SQL,
)
from taskq.workflows._sql_ledger import (
    FLOW_RUN_INSERT_SQL,
    FLOW_RUN_READ_SQL,
    LEDGER_CLAIM_SQL,
    LEDGER_FENCE_ATTEMPT_SQL,
    LEDGER_FENCE_BY_ID_SQL,
    LEDGER_MEMOIZED_SQL,
    LEDGER_TERMINAL_BY_ID_SQL,
    LEDGER_TERMINAL_SQL,
)
from taskq.workflows._sql_progress import (
    PROGRESS_CHILD_RESULTS_SQL,
    PROGRESS_REPLAY_NODE_SQL,
    PROGRESS_REPLAY_RUN_SQL,
    PROGRESS_RING_OLDEST_NODE_SQL,
    PROGRESS_RING_OLDEST_RUN_SQL,
    PROGRESS_RING_PRUNE_SQL,
    PROGRESS_STATE_READ_NODE_SQL,
    PROGRESS_STATE_READ_RUN_SQL,
    PROGRESS_STATE_UPSERT_SQL,
    PROGRESS_STREAM_APPEND_TRIM_SQL,
)
from taskq.workflows._sql_status import (
    WORKFLOW_MAP_PROGRESS_SQL,
    WORKFLOW_NODES_SQL,
    WORKFLOW_ROLLUP_SQL,
    WORKFLOW_ROOT_MAINTAIN_SQL,
)
from taskq.workflows._sql_sweep import (
    JOIN_BODY_UNAVAILABLE_SQL,
    NODELESS_ROOT_REAP_SQL,
    OUTBOX_DRAIN_CONSUMERS_SQL,
    OUTBOX_DRAIN_FLIP_SQL,
    OUTBOX_FETCH_UNDELIVERED_SQL,
    PHANTOM_REAP_SQL,
    REDERIVE_SWEEP_SQL,
    SWEEP_FIRE_SQL,
)

__all__ = [
    "BLOCKING_REASON_JOIN",
    "BLOCKING_REASON_ORPHAN_PARENT",
    "TERMINAL_SQL_SET",
    "WorkflowSql",
]

#: The terminal-status SQL set — the statement-side twin of
#: :data:`taskq.backend.statemachine.TERMINAL_STATUSES`. A literal (not a
#: bind) because it is fixed vocabulary, never caller input; the equivalence
#: to the Python frozenset is pinned by test.
TERMINAL_SQL_SET: Final[str] = "('succeeded','failed','cancelled','crashed','abandoned')"

#: metadata.blocking_reason values (the blocked-row representation, T03:
#: carried on the row's metadata jsonb, never a new ENUM).
BLOCKING_REASON_JOIN: Final[str] = "join"
BLOCKING_REASON_ORPHAN_PARENT: Final[str] = "orphan_parent"
#: The fail-closed block (T06): a join whose parent TERMINAL-failed — the
#: joined node's side of the counter is RESOLVED by this stamp (the
#: rederive locks 'join' rows only); metadata.failed_parent names it.
BLOCKING_REASON_FAILED_PARENT: Final[str] = "failed_parent"
#: The loudness stamp (R2-2): a fired join whose reducer body resolved
#: NOWHERE — the consumers' delivery continues (the delivery contract),
#: but the record names the defect instead of looking healthy.
BLOCKING_REASON_BODY_UNAVAILABLE: Final[str] = "body_unavailable"
#: The flow-fenced join (the phase-2 attack's H2): a never-fired join row
#: whose resolution the FLOW'S OWN DEATH fenced — the fire's flow-status
#: leg refuses a terminal flow, so the join can never fire again. The
#: sweep's flow-fenced arm stamps it (the blocked-with-reason terminal
#: state); a join with a FAILED parent is stamped 'failed_parent' (the
#: failure is the cause) — this name is for the join whose parents all
#: terminalized fine and whose fire the flow's death fenced anyway.
BLOCKING_REASON_FLOW_DEAD: Final[str] = "flow_dead"


@dataclass(frozen=True, slots=True)
class WorkflowSql:
    """The workflow statement bundle, rendered for one validated schema."""

    schema: str
    terminal_mark: str
    decrement: str
    decrement_absorbed: str
    fail_closed_cascade: str
    collect_fan_in: str
    collect_fan_in_append: str
    fire: str
    outbox_insert: str
    outbox_fetch_undelivered: str
    outbox_drain_consumers: str
    outbox_drain_flip: str
    fork_children: str
    fork_edges: str
    emit_children: str
    emit_edges: str
    emit_cursor: str
    fork_join_node: str
    fork_join_consumers: str
    fork_consumer_edges: str
    rederive_sweep: str
    sweep_fire: str
    join_body_unavailable: str
    ledger_claim: str
    ledger_memoized: str
    ledger_terminal: str
    ledger_terminal_by_id: str
    ledger_fence_attempt: str
    ledger_fence_by_id: str
    phantom_reap: str
    nodeless_root_reap: str
    node_insert: str
    node_edge: str
    flow_status: str
    flow_run_insert: str
    flow_run_read: str
    workflow_rollup: str
    workflow_nodes: str
    workflow_map_progress: str
    workflow_root_maintain: str
    # T21's progress surface (the two-channel persistence + the faces).
    progress_state_upsert: str
    progress_stream_append: str
    progress_ring_prune: str
    progress_replay_node: str
    progress_replay_run: str
    progress_ring_oldest_node: str
    progress_ring_oldest_run: str
    progress_state_read_node: str
    progress_state_read_run: str
    progress_child_results: str

    @staticmethod
    def build(schema: str) -> WorkflowSql:
        """Render every constant for *schema* (validated via
        ``taskq.constants.require_schema``).

        All user-supplied values use ``$N`` parameter binding — only the
        schema identifier is interpolated, and it is validated here before
        any statement renders (the S608 rationale, the same discipline as
        ``backend/_sql_templates.render``).
        """
        require_schema(schema)
        subs: Final[tuple[tuple[str, str], ...]] = (
            ("{schema}", schema),
            ("{terminal}", TERMINAL_SQL_SET),
        )

        def render(template: str) -> str:
            out = template
            for token, value in subs:
                out = out.replace(token, value)
            # The doubled braces in the jsonb shape literals are the
            # migration-runner convention; substitution needs them single.
            return out.replace("{{", "{").replace("}}", "}")

        return WorkflowSql(
            schema=schema,
            terminal_mark=render(TERMINAL_MARK_SQL),
            decrement=render(DECREMENT_SQL),
            decrement_absorbed=render(DECREMENT_ABSORBED_SQL),
            fail_closed_cascade=render(FAIL_CLOSED_CASCADE_SQL),
            collect_fan_in=render(COLLECT_FAN_IN_SQL),
            collect_fan_in_append=render(COLLECT_FAN_IN_APPEND_SQL),
            fire=render(FIRE_SQL),
            outbox_insert=render(OUTBOX_INSERT_SQL),
            outbox_fetch_undelivered=render(OUTBOX_FETCH_UNDELIVERED_SQL),
            outbox_drain_consumers=render(OUTBOX_DRAIN_CONSUMERS_SQL),
            outbox_drain_flip=render(OUTBOX_DRAIN_FLIP_SQL),
            fork_children=render(FORK_CHILDREN_SQL),
            fork_edges=render(FORK_EDGES_SQL),
            emit_children=render(EMIT_CHILDREN_SQL),
            emit_edges=render(EMIT_EDGES_SQL),
            emit_cursor=render(EMIT_CURSOR_SQL),
            fork_join_node=render(FORK_JOIN_NODE_SQL),
            fork_join_consumers=render(FORK_JOIN_CONSUMERS_SQL),
            fork_consumer_edges=render(FORK_CONSUMER_EDGES_SQL),
            rederive_sweep=render(REDERIVE_SWEEP_SQL),
            sweep_fire=render(SWEEP_FIRE_SQL),
            join_body_unavailable=render(JOIN_BODY_UNAVAILABLE_SQL),
            ledger_claim=render(LEDGER_CLAIM_SQL),
            ledger_memoized=render(LEDGER_MEMOIZED_SQL),
            ledger_terminal=render(LEDGER_TERMINAL_SQL),
            ledger_terminal_by_id=render(LEDGER_TERMINAL_BY_ID_SQL),
            ledger_fence_attempt=render(LEDGER_FENCE_ATTEMPT_SQL),
            ledger_fence_by_id=render(LEDGER_FENCE_BY_ID_SQL),
            phantom_reap=render(PHANTOM_REAP_SQL),
            nodeless_root_reap=render(NODELESS_ROOT_REAP_SQL),
            node_insert=render(NODE_INSERT_SQL),
            node_edge=render(NODE_EDGE_SQL),
            flow_status=render(FLOW_STATUS_SQL),
            flow_run_insert=render(FLOW_RUN_INSERT_SQL),
            flow_run_read=render(FLOW_RUN_READ_SQL),
            workflow_rollup=render(WORKFLOW_ROLLUP_SQL),
            workflow_nodes=render(WORKFLOW_NODES_SQL),
            workflow_map_progress=render(WORKFLOW_MAP_PROGRESS_SQL),
            workflow_root_maintain=render(WORKFLOW_ROOT_MAINTAIN_SQL),
            progress_state_upsert=render(PROGRESS_STATE_UPSERT_SQL),
            progress_stream_append=render(PROGRESS_STREAM_APPEND_TRIM_SQL),
            progress_ring_prune=render(PROGRESS_RING_PRUNE_SQL),
            progress_replay_node=render(PROGRESS_REPLAY_NODE_SQL),
            progress_replay_run=render(PROGRESS_REPLAY_RUN_SQL),
            progress_ring_oldest_node=render(PROGRESS_RING_OLDEST_NODE_SQL),
            progress_ring_oldest_run=render(PROGRESS_RING_OLDEST_RUN_SQL),
            progress_state_read_node=render(PROGRESS_STATE_READ_NODE_SQL),
            progress_state_read_run=render(PROGRESS_STATE_READ_RUN_SQL),
            progress_child_results=render(PROGRESS_CHILD_RESULTS_SQL),
        )
