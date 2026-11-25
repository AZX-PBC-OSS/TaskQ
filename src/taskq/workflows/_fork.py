"""The fork (the fork-atomicity rule): the parent's guarded terminal
UPDATE + the fork's child INSERTs + the join-row INSERT share one
transaction — the writes land inside the CALLER's tx1 (never a second
transaction, which would break the atomicity rule 5 and reopen the
fork-debt window pin 19 rejects).

Fan-out inserts are chunked parallel-array statements (``_FORK_CHUNK``
rows per statement — never one round trip per child; the 1000-child
fan-out tx band's shape). Ids are minted per fork through the
``taskq._ids`` seam (uuid7, time-ordered); the wiring lives in
``(parent_id, map_index)`` — never in a string-shape convention on
hand-built ids (pin 18's collision dragon).
"""

from __future__ import annotations

import structlog

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, JobId
from taskq.constants import SMALLINT_MAX
from taskq.obs import get_logger
from taskq.workflows._policy import canonical_retry_kind, row_retry_curve
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import (
    MAP_INDEX_CEILING,
    ForkSpec,
    _join_metadata,
    _jsonb,
    _row_metadata,
    resolve_map_index_domain,
    validate_map_index_identity,
)
from taskq.workflows.definitions import FanInBoundExceededError, validate_fork

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

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
    app-side (uuid7 via the seam). never a second transaction: the parent's
    terminal mark, the children, and the join share one tx, one now()."""
    validate_fork(fork)  # the empty fork is a build-time refusal, never a runtime dragon
    children = fork.children
    for child in children:
        if child.map_index is not None:
            validate_map_index_identity(child.map_index)
    consumers = fork.join.consumers if fork.join is not None else ()
    for consumer in consumers:
        if consumer.map_index is not None:
            validate_map_index_identity(consumer.map_index)
    high = [
        child.map_index
        for child in children
        if child.map_index is not None and child.map_index > MAP_INDEX_CEILING
    ] + [
        consumer.map_index
        for consumer in consumers
        if consumer.map_index is not None and consumer.map_index > MAP_INDEX_CEILING
    ]
    if high:
        # One caller-connection/all-five read per fork. A consistent bigint
        # schema admits high identities (including BIGINT_MAX); all-smallint
        # keeps the old refusal, while missing/unknown/mixed schemas fail closed
        # before any child, edge, join or consumer write.
        from taskq.workflows._emit import MapIndexExhaustedError

        bound = await resolve_map_index_domain(conn, wsql.schema)
        poison = [value for value in high if value > bound]
        if poison:
            raise MapIndexExhaustedError(
                f"the fork's children/consumers carry map_index {poison!r} past "
                f"the schema's resolved five-carrier upper bound ({bound}) — "
                "the fork refuses BEFORE writing children, edges or a join"
            )
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
            [c.resolved_max_attempts or fork.max_attempts for c in chunk],
            [c.resolved_retry_kind or canonical_retry_kind(fork.retry_kind) for c in chunk],
            [parent_id] * len(chunk),
            [c.map_index for c in chunk],
            [c.step_key for c in chunk],
            [fork.trace_id] * len(chunk),
            # per-child metadata: a child declaring admission terms (fix
            # 2 — the arm's/map's bucket names, and the strict
            # reservation names beside them — the own ride) carries
            # them on its own row; the no-terms rows keep the base shape
            # byte-identical.
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
            [f"workflow:{flow_id}"] * len(chunk),
            # The key is parent-scoped (the wiring identity: parent node +
            # child key + map index) — a bare (flow, child step) key would
            # collide across every fork of the same step (pin 18's dragon).
            [
                f"wf:{flow_id}:{parent_step_key}:{c.step_key}"
                + (f":{c.map_index}" if c.map_index is not None else "")
                for c in chunk
            ],
            # The children's resolved priority (the priority surface):
            # each child's own resolution rides its row; the unset case
            # coalesces to the column default's own value (the pre-fix
            # byte-identical row).
            [c.priority if c.priority is not None else 0 for c in chunk],
            # The children's declared deadline: each child's own
            # resolution rides its row; the
            # unset case binds NULL (the column's own default — the
            # pre-fix byte-identical row).
            [c.start_to_close for c in chunk],
            # The children's dispatch cohort (the fairness stamp): one
            # cohort per run (the flow_id) — the round-robin dispatch
            # interleaves runs instead of draining them as one FIFO
            # stream.
            [str(flow_id)] * len(chunk),
            *[[row_retry_curve(c.retry_policy)[column] for c in chunk] for column in range(4)],
        )
        await conn.execute(
            wsql.fork_edges,
            ids,
            [parent_id] * len(chunk),
            [flow_id] * len(chunk),
            # The fork's own child edges carry the default policy: these
            # edges reconcile fork debt; the propagation rule reads
            # the JOIN's incoming edges only.
            ["fail_closed"] * len(chunk),
        )

    join_id: JobId | None = None
    if fork.join is not None:
        # The DSL never pre-inserts a join (create_flow skips map joins), and
        # since #745 no binding names one. The probe can still find a row
        # minted by an outbox drain from a binding stamped before #745 (an
        # in-flight run), or one an engine-level caller inserted itself
        # through insert_node. The fork ADOPTS a PENDING one: add the items'
        # weight to the counter + stamp the consumers, never a second insert.
        # The status leg is load-bearing: adopting a row that already ran
        # leaves the consumers' reserved deps unreleased (the run wedges or
        # reads an empty join), so any other state is left untouched — the
        # fork takes the insert path and its own row runs with the true
        # children. The read is ordered so a pre-fix residue (several rows
        # for the key) resolves deterministically: the lowest PENDING row
        # wins, and a non-pending row is read only to report it.
        join_row = await conn.fetchrow(
            f"SELECT id, status, deps_pending FROM {wsql.schema}.jobs WHERE step_key = $1 "  # noqa: S608  # Why: the schema is the bundle's validated identifier; every value is $-bound.
            "AND (metadata->>'flow_id')::uuid = $2 "
            "ORDER BY (status = 'pending') DESC, id LIMIT 1",
            fork.join.step_key,
            flow_id,
        )
        if join_row is not None and join_row["status"] == "pending":
            join_id = JobId(join_row["id"])
            logger.debug(
                "wf-fork-join-adopted",
                flow_id=str(flow_id),
                step_key=fork.join.step_key,
                existing_job_id=str(join_id),
            )
            # The counter is a COUNT of unterminal parents (a smallint, never
            # the widened record identity), and the pre-created join already
            # carries its own source edge's weight: the adoption adds the
            # children's to it, so the domain check must read the row's
            # current value — refusing typed before the UPDATE writes.
            existing_deps = int(join_row["deps_pending"])
            if existing_deps + len(children) > SMALLINT_MAX:
                raise FanInBoundExceededError(
                    f"join {fork.join.step_key!r} adoption adds {len(children)} "
                    f"children to {existing_deps} existing deps — above "
                    f"jobs.deps_pending's smallint domain ({SMALLINT_MAX}); the "
                    "adoption refuses before the counter write, and the caller's "
                    "transaction unwinds the fork's child inserts. "
                    "Partition the source into smaller forks — widening the "
                    "record identity does not widen the join counter."
                )
            # the pre-created row (the drain-precreated shape — the flow link +
            # the cohort stamp, no join-wait marker) leaves the adoption UPDATE
            # carrying the marker and the run's cohort (the source row's
            # resolved names), so a nested fan-out over this join reads the
            # stamp — the depth-N induction holds through adoption too.
            join_doc = _join_metadata(
                flow_id,
                (),
                child_driven=fork.join.child_driven,
                reservations=fork.join.reservations,
            )
            join_doc.pop("flow_id", None)
            await conn.execute(
                f"UPDATE {wsql.schema}.jobs "  # noqa: S608  # Why: the schema is the bundle's validated identifier; every value is a bound parameter.
                "SET deps_pending = deps_pending + $1, "
                "metadata = jsonb_set(metadata || $2::jsonb, '{consumers}', $3::jsonb, true) "
                "WHERE id = $4",
                len(children),
                _jsonb(join_doc),
                _jsonb(
                    _join_metadata(
                        flow_id, fork.join.consumers, child_driven=fork.join.child_driven
                    ).get("consumers")
                    or []
                ),
                join_id,
            )
        else:
            if join_row is not None:
                # The row exists and has already run (or is running): adopting
                # it would leave every consumer's reserved dep unreleased —
                # the wedge the old status-less probe caused. The residue is
                # otherwise invisible to the upgrade detectors (a
                # never-adopted row has deps_pending = 0), so this is the one
                # place it is reported from the system's own records.
                logger.warning(
                    "wf-fork-join-adoption-skipped",
                    flow_id=str(flow_id),
                    step_key=fork.join.step_key,
                    existing_job_id=str(join_row["id"]),
                    existing_status=str(join_row["status"]),
                )
            join_id = JobId(new_uuid())
            await conn.execute(
                wsql.fork_join_node,
                join_id,
                fork.join.actor,
                fork.join.queue,
                _jsonb(fork.join.payload),
                fork.max_attempts,
                canonical_retry_kind(fork.retry_kind),
                parent_id,
                fork.join.step_key,
                len(children),
                fork.trace_id,
                _jsonb(
                    _join_metadata(
                        flow_id,
                        fork.join.consumers,
                        child_driven=fork.join.child_driven,
                        # the join carries them so a nested fan-out over this
                        # join reads the stamp — the depth-N cohort guarantee.
                        reservations=fork.join.reservations,
                    )
                ),
                f"workflow:{flow_id}",
                f"wf:{flow_id}:{parent_step_key}:{fork.join.step_key}",
                # The join's resolved priority (the priority surface): the
                # source's resolution — the join is the source's own
                # cohort (the placement fields say so); the arms'
                # overrides apply to the arm children, never the join.
                # The unset case coalesces to the column default's own
                # value (the pre-fix byte-identical row).
                fork.priority if fork.priority is not None else 0,
                # The join's declared deadline: the source's resolution
                # (the join is the
                # source's own cohort); the unset case binds NULL.
                fork.start_to_close,
                # The join's dispatch cohort (the fairness stamp):
                # the run's flow_id — the join is claimable work like
                # any node of the run.
                str(flow_id),
            )
        # The join's edges: one per child (the ledger's truth — the
        # fan-out's children feed the join).
        for start in range(0, len(child_ids), FORK_CHUNK):
            chunk_ids = child_ids[start : start + FORK_CHUNK]
            await conn.execute(
                wsql.fork_edges,
                # child_id = the join; parent_id = each child. each edge
                # records the JOIN'S declared failure policy: the
                # propagation rule reads it off the ledger — the absorption
                # is on the record, never inferred.
                [join_id] * len(chunk_ids),
                chunk_ids,
                [flow_id] * len(chunk_ids),
                [fork.join.failure_policy] * len(chunk_ids),
            )
        # the join's downstream consumers are wired like any node — the
        # consumer's dep was reserved at create (the runner's static insert
        # counts the fork-spawned join's edge before the join's id exists), so
        # this edge row is the ledger's truth that releases it: the consumer
        # dispatches strictly after the JOIN'S own terminal (the join row's
        # claim + default packer writes the collected result), and the
        # consumer's arg resolution reads the join's result through the same
        # typed door as any node result (the parent-results query on the edge
        # ledger). The outbox's consumer insert stays as the belt (the
        # arbiter's idempotency key conflicts with the static row — no double
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
            # a join born deps_pending=0 never runs the decrement path (there
            # are no children to decrement it), so its fire went unledgered —
            # the empty route's consumer dispatched and the exactly-once
            # receipt never existed. The same guarded INSERT the decrement path
            # fires (the PK's ON CONFLICT: idempotent; the guards:
            # deps_pending=0, the row pending, the flow alive) mints the
            # receipt at the birth; the consumer's dispatch stays the spawn's
            # own path.
            await conn.execute(wsql.fire, join_id, flow_id, new_uuid(), "born-zero")
    return child_ids, join_id
