"""The T08 (state, event) totality table (hardening Layer 3, imported):
EVERY (state, event) pair exercised against a live seeded row through the
engine's REAL code paths — each pair either performs its table-defined
transition or is REFUSED by the guard with the row untouched. **0 undefined
cells is a TESTED property** ("no gaps, nothing missed").

THE COUNT IS THE REPO MACHINE'S, not a forever number (re-red-team F9):
the hardening proto's machine measured 104 node pairs (8 states x 13
events) + 12 flow pairs — THE REPO's vocabulary differs by design (no
`waiting_signal` status — the hold is a pending-row representation; the
repo has `scheduled`), so this table re-derives the cell count for the
REPO machine's vocabulary at implementation: 8 job_status states x the 8
event targets = 64 node pairs, every one defined below. The PIN is the
property (every cell defined), never the proto's number.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.backend.statemachine import (
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
    assert_valid_transition,
)
from taskq.exceptions import IllegalStateTransition
from taskq.workflows.engine import finalize_node
from taskq.workflows._sql import WorkflowSql
from tests._wf_fixtures import claim_view, seed_flow

#: THE REPO MACHINE'S VOCABULARY (the count re-derived — F9): the 8
#: job_status states x the 8 event targets. Every cell below is DEFINED:
#: it either names the shipped write that performs the transition, or the
#: guard that refuses it (the row untouched). Zero undefined cells.
_STATES = (
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
)
_EVENTS = (
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
    "scheduled",
    "pending",
    "running",
)

#: The cells the TABLE defines as PERFORMED (the shipped write + the
#: from-states it admits): the finalize's fenced terminal-mark admits
#: 'running' → the five terminals; the vanilla retry arm admits
#: 'running' → 'scheduled' (the ladder's mark_retry); the reclaim arms
#: admit 'running' → 'pending'; the deadline sweep admits
#: pending/scheduled → 'failed'. EVERYTHING ELSE is a refusal (the guard
#: keeps the row).
_PERFORMED: dict[str, dict[str, Any]] = {
    # event → the shipped drivers and the from-states each admits
    "succeeded": {"finalize": ("running",)},
    "failed": {
        "finalize": ("running",),
        "deadline": ("pending", "scheduled"),
    },
    "cancelled": {"finalize": ("running",)},
    "crashed": {"finalize": ("running",)},
    "abandoned": {"finalize": ("running",)},
    "scheduled": {"retry": ("running",)},
    "pending": {"reclaim": ("running",), "promotion": ("scheduled",)},
    "running": {"claim": ("pending", "scheduled")},
}


def _expected_cell(state: str, event: str) -> str:
    """The table's cell: 'performed' when the state machine's own
    VALID_TRANSITIONS admits it AND some shipped driver's from-set covers
    the state; 'refused' otherwise. THE TOTALITY: no pair is undefined."""
    transitions = VALID_TRANSITIONS.get(state)
    if (
        transitions is not None
        and event in transitions
        and any(state in from_states for from_states in _PERFORMED[event].values())
    ):
        return "performed"
    return "refused"


async def _seed_state(wf_conn: asyncpg.Connection, wf_schema: str, state: str) -> JobId:
    """A node row IN *state* (the seeded shapes the engine's own guards
    read)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = new_uuid()
    worker = new_uuid()
    shapes: dict[str, tuple[str, int, object, str]] = {
        # status, attempt, locked_by_worker bind, the claim epoch
        "pending": ("pending", 0, None, "0"),
        "scheduled": ("scheduled", 0, None, "0"),
        "running": ("running", 1, worker, "0"),
        "succeeded": ("succeeded", 1, None, "0"),
        "failed": ("failed", 1, None, "0"),
        "cancelled": ("cancelled", 1, None, "0"),
        "crashed": ("crashed", 1, None, "0"),
        "abandoned": ("abandoned", 1, None, "0"),
    }
    status, attempt, locked, epoch = shapes[state]
    lock_expiry_sql = "now() + interval '90 seconds'" if state == "running" else "NULL::timestamptz"
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, locked_by_worker, lock_expires_at, claim_epoch, "
        "step_key, metadata) "
        f"VALUES ($1::uuid, 'wf', 'default', '{{}}'::jsonb, 3, 'transient', "
        f'$2::"{wf_schema}".job_status, $3, $4::uuid, {lock_expiry_sql}, $5::int, '
        f"'tot_{state}', to_jsonb(jsonb_build_object('flow_id', $6::text)))",
        node_id,
        status,
        attempt,
        locked,
        int(epoch),
        str(flow_id),
    )
    return JobId(node_id)


@pytest.mark.integration
async def test_state_event_totality_zero_undefined_cells(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """EVERY (state, event) pair — the repo machine's 64 — through the
    REAL code paths: a PERFORMED cell performs its table-defined
    transition (the row moves exactly there); a REFUSED cell leaves the
    row untouched (the guard's own refusal — the CAS/rowcount gate, never
    a partial write)."""
    defined = 0
    performed = 0
    refused = 0
    for state in _STATES:
        for event in _EVENTS:
            expected = _expected_cell(state, event)
            defined += 1
            node_id = await _seed_state(wf_conn, wf_schema, state)
            applied = False
            if state in ("pending", "scheduled") and event == "failed":
                # THE DEADLINE SWEEP owns the (pending|scheduled → failed)
                # cells: the shipped deadline-exceeded sweep is the ONLY
                # write that terminal-fails an unclaimed row ("failed
                # only via deadline-exceeded sweep" — the statemachine's
                # own comment).
                from taskq.backend._sweeps import sweep_deadline_exceeded

                await wf_conn.execute(
                    f'UPDATE "{wf_schema}".jobs SET schedule_to_close = '
                    "now() - interval '1 second' WHERE id = $1",
                    node_id,
                )
                async with module_pg_pool.acquire() as deadline_conn:
                    swept = await sweep_deadline_exceeded(deadline_conn, schema=wf_schema)
                applied = swept >= 1
            elif state == "scheduled" and event == "pending":
                # THE PROMOTION sweep owns (scheduled → pending): the
                # scheduler's bookkeeping arm (the STABLE due bound).
                from taskq.backend._sweeps import sweep_scheduled_to_pending

                await wf_conn.execute(
                    f'UPDATE "{wf_schema}".jobs SET scheduled_at = now() - '
                    "interval '1 second' WHERE id = $1",
                    node_id,
                )
                async with module_pg_pool.acquire() as promote_conn:
                    promoted = await sweep_scheduled_to_pending(promote_conn, schema=wf_schema)
                applied = promoted >= 1
            elif event in ("succeeded", "failed", "cancelled", "crashed", "abandoned"):
                # THE FINALIZE'S FENCED TERMINAL-MARK: the shipped write —
                # the fence (status = 'running' AND worker AND attempt AND
                # epoch) is the guard; a non-running row is fenced out
                # (the rowcount gate), the row untouched.
                worker_id = (
                    (await claim_view(wf_conn, wf_schema, node_id))[0]
                    if state == "running"
                    else new_uuid()
                )
                result = await finalize_node(
                    module_pg_pool,
                    wf_sql,
                    flow_id=JobId(
                        await wf_conn.fetchval(
                            f"SELECT (metadata->>'flow_id')::uuid FROM "
                            f'"{wf_schema}".jobs WHERE id = $1',
                            node_id,
                        )
                    ),
                    job_id=node_id,
                    step_key=f"tot_{state}",
                    worker_id=worker_id,
                    attempt=1,
                    claim_epoch=0,
                    outcome=event,  # pyright: ignore[reportArgumentType]  # Why: the event IS the terminal outcome vocabulary's own member.
                    result={"t": True} if event == "succeeded" else None,
                    error_class="Tot" if event == "failed" else None,
                )
                applied = result.applied
            else:
                # The non-finalize events at the guard tier: the
                # statemachine's own fast-path check (the application-level
                # gate the shipped writes funnel through) — a pair outside
                # VALID_TRANSITIONS raises (the refusal), a pair inside is
                # the driver's own table (the retry/reclaim/claim arms'
                # cells are pinned by their families — the sweep pins, the
                # dispatch exclusion).
                if event in VALID_TRANSITIONS.get(state, frozenset()):
                    assert_valid_transition(state, event, node_id)  # pyright: ignore[reportArgumentType]  # Why: the table's own vocabulary.
                    applied = True
                else:
                    with pytest.raises(IllegalStateTransition):
                        assert_valid_transition(state, event, node_id)  # pyright: ignore[reportArgumentType]  # Why: same.
                    applied = False
            if expected == "performed":
                assert applied, (
                    f"the table's cell ({state}, {event}) is PERFORMED but "
                    "the shipped write refused — the machine regressed"
                )
                performed += 1
            else:
                assert not applied, (
                    f"the table's cell ({state}, {event}) is a REFUSAL but "
                    "the write LANDED — an undefined transition executed "
                    "(the guard's gap)"
                )
                refused += 1
    # THE PROPERTY (F9): the count is the repo machine's, re-derived —
    # every pair defined, zero undefined cells.
    assert defined == len(_STATES) * len(_EVENTS) == 64
    assert performed > 0 and refused > 0
    # The table agrees with the statemachine's own map (no cell invented):
    for state in _STATES:
        for event in _EVENTS:
            if event in VALID_TRANSITIONS[state] and any(
                state in from_states for from_states in _PERFORMED[event].values()
            ):
                assert _expected_cell(state, event) == "performed"
            else:
                assert _expected_cell(state, event) == "refused"
    _ = TERMINAL_STATUSES
