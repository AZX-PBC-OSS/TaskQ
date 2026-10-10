"""The fork (T04's fork-atomicity rule): the parent's guarded terminal
UPDATE + the fork's child INSERTs + the join-row INSERT share ONE
transaction — the writes land inside the CALLER's tx1 (never a second
transaction, which would break the atomicity rule 5 and reopen the
fork-debt window pin 19 convicts).

Fan-out inserts are chunked parallel-array statements (``_FORK_CHUNK``
rows per statement — never one round trip per child; the 1000-child
fan-out tx band's shape). Ids are minted per fork through the
``taskq._ids`` seam (uuid7, time-ordered); the wiring lives in
``(parent_id, map_index)`` — NEVER in a string-shape convention on
hand-built ids (pin 18's collision dragon).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import ForkSpec, _join_metadata, _jsonb, _metadata, _row_metadata
from taskq.workflows.definitions import validate_fork

if TYPE_CHECKING:
    pass

__all__ = ["FORK_CHUNK", "insert_fork"]


#: Fan-out chunk size: rows per parallel-array INSERT statement.
FORK_CHUNK = 500


async def insert_fork(
    conn: ConnLike,
    wsql: WorkflowSql,
    *,
    flow_id: JobId,
    parent_id: JobId,
    parent_step_key: str,
    fork: ForkSpec,
) -> tuple[list[JobId], JobId | None]:
    """The fork's writes, inside the CALLER's transaction (tx1): the child
    rows, the edge rows, the join node — parallel-array chunks, ids minted
    app-side (uuid7 via the seam). NEVER a second transaction: the parent's
    terminal mark, the children, and the join share one tx, one now()."""
    validate_fork(fork)  # the empty fork is a build-time refusal, never a runtime dragon
    children = fork.children
    child_ids = [JobId(new_uuid()) for _ in children]

    for start in range(0, len(children), FORK_CHUNK):
        chunk = children[start : start + FORK_CHUNK]
        ids = child_ids[start : start + FORK_CHUNK]
        await conn.execute(
            wsql.fork_children,
            ids,
            [c.actor for c in chunk],
            [c.queue for c in chunk],
            [_jsonb(c.payload) for c in chunk],
            [fork.max_attempts] * len(chunk),
            [fork.retry_kind] * len(chunk),
            [parent_id] * len(chunk),
            [c.map_index for c in chunk],
            [c.step_key for c in chunk],
            [fork.trace_id] * len(chunk),
            # PER-CHILD metadata: a child declaring admission terms (CURE
            # 2 — the arm's/map's bucket names) carries them on its OWN
            # row; the no-bucket rows keep the base shape byte-identical.
            [
                _jsonb(_row_metadata(flow_id, rate_limits=c.rate_limits) if c.rate_limits else _metadata(flow_id, blocking_reason=None))
                for c in chunk
            ],
            [f"workflow:{flow_id}"] * len(chunk),
            # The key is PARENT-SCOPED (the wiring identity: parent node +
            # child key + map index) — a bare (flow, child step) key would
            # collide across every fork of the same step (pin 18's dragon).
            [
                f"wf:{flow_id}:{parent_step_key}:{c.step_key}"
                + (f":{c.map_index}" if c.map_index is not None else "")
                for c in chunk
            ],
        )
        await conn.execute(
            wsql.fork_edges,
            ids,
            [parent_id] * len(chunk),
            [flow_id] * len(chunk),
            # The fork's own child edges carry the DEFAULT policy: these
            # edges reconcile fork debt; the propagation rule (T06) reads
            # the JOIN's incoming edges only.
            ["fail_closed"] * len(chunk),
        )

    join_id: JobId | None = None
    if fork.join is not None:
        # THE ADOPTION PROBE (attack4's full-static-insert cure): the
        # API's create_flow pre-inserted EVERY node — the join included
        # (deps 1: its source edge). When the row exists, the fork
        # ADOPTS it: add the items' weight to the counter + stamp the
        # consumers — never a second insert (the plain insert's
        # idempotency collision killed the second demo run's fork). The
        # engine's raw shapes (no static row) keep the insert path.
        join_id = await conn.fetchval(
            f"SELECT id FROM {wsql.schema}.jobs WHERE step_key = $1 "  # noqa: S608  # Why: the schema is the bundle's validated identifier; every value is $-bound.
            "AND (metadata->>'flow_id')::uuid = $2",
            fork.join.step_key,
            flow_id,
        )
        if join_id is not None:
            await conn.execute(
                f"UPDATE {wsql.schema}.jobs "  # noqa: S608  # Why: the schema is the bundle's validated identifier; every value is a bound parameter.
                "SET deps_pending = deps_pending + $1, "
                "metadata = jsonb_set(metadata, '{consumers}', $2::jsonb, true) "
                "WHERE id = $3",
                len(children),
                _jsonb(
                    _join_metadata(
                        flow_id, fork.join.consumers, child_driven=fork.join.child_driven
                    ).get("consumers")
                    or []
                ),
                join_id,
            )
        else:
            join_id = JobId(new_uuid())
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
                _jsonb(
                    _join_metadata(
                        flow_id, fork.join.consumers, child_driven=fork.join.child_driven
                    )
                ),
                f"workflow:{flow_id}",
                f"wf:{flow_id}:{parent_step_key}:{fork.join.step_key}",
            )
        # The join's edges: one per child (the ledger's truth — the
        # fan-out's children feed the join).
        for start in range(0, len(child_ids), FORK_CHUNK):
            chunk_ids = child_ids[start : start + FORK_CHUNK]
            await conn.execute(
                wsql.fork_edges,
                # child_id = the join; parent_id = each child. EACH EDGE
                # RECORDS THE JOIN'S DECLARED FAILURE POLICY (T06): the
                # propagation rule reads it off the ledger — the absorption
                # is on the record, never inferred.
                [join_id] * len(chunk_ids),
                chunk_ids,
                [flow_id] * len(chunk_ids),
                [fork.join.failure_policy] * len(chunk_ids),
            )
        # THE FORK'S CONSUMER EDGES (the map-join consumption cure — the
        # ecosystem mapper's defect): the join's DOWNSTREAM consumers are
        # wired like any node — the consumer's dep was RESERVED at create
        # (the runner's static insert counts the fork-spawned join's edge
        # before the join's id exists), so THIS edge row is the ledger's
        # truth that releases it: the consumer dispatches strictly after
        # the JOIN'S OWN TERMINAL (the join row's claim + default packer
        # writes the collected result), and the consumer's arg resolution
        # reads the join's result through the SAME typed door as any node
        # result (the parent-results query on the edge ledger). The
        # outbox's consumer insert stays as the belt (the arbiter's
        # idempotency key conflicts with the static row — no double
        # dispatch).
        if fork.join.consumers:
            await conn.execute(
                wsql.fork_consumer_edges,
                join_id,
                flow_id,
                [c.step_key for c in fork.join.consumers],
                [c.failure_policy for c in fork.join.consumers],
            )
        if not children:
            # THE BORN-ZERO JOIN'S FIRE RECEIPT (rv4 F1's exactly-once
            # law): a join born deps_pending=0 never runs the decrement
            # path (there are no children to decrement it), so its fire
            # went UNLEDGERED — the empty route's consumer dispatched and
            # the exactly-once receipt never existed. The SAME guarded
            # INSERT the decrement path fires (the PK's ON CONFLICT:
            # idempotent; the guards: deps_pending=0, the row pending,
            # the flow alive) mints the receipt at the birth; the
            # consumer's dispatch stays the spawn's own path.
            await conn.execute(wsql.fire, join_id, flow_id, new_uuid(), "born-zero")
    return child_ids, join_id
