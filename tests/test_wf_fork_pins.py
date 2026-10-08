"""The fork-family pins (T04): pin 18 (ID-COLLISION — the compiled graph owns ids), pin 19 (FORK-DEBT — the atomic fork makes the debt unrepresentable; the split-tx variant reds), pin 20 (OUTBOX-FLUSH — the drain completes the dispatch exactly once).

Driven against a live Postgres through the REAL engine; the shared seed
helpers + fixtures live in ``tests/_wf_fixtures.py`` (the composed-fixture
home), the red-output sink flushes to ``.measurements/pin-reds.json`` (a
file that gets READ — BUILD-PROTOCOL §2), the shipped invariants green,
the unfenced variants kept in this file forever as the convicted shapes.
"""

# Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
# Why: random module used for timing jitter in race tests, not crypto.

from __future__ import annotations

import itertools
import uuid
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._types import ChildSpec, ConsumerBinding, ForkSpec, JoinSpec
from taskq.workflows._fork import insert_fork
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._sweep import drain_outbox
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import (
    RedLog,
    claim_view,
    node_state,
    seed_flow,
    seed_running_node,
)

# ── Pin 18: ID-COLLISION (the compiled graph owns ids) ──────────────────


@pytest.mark.integration
async def test_pin_18_no_id_collision(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fork whose children derive ids from a ``{parent}.{child}``
    string-shape convention over hand-built parent ids containing dots
    lands ALL forks on the SAME child ids (the silently wrong graph). The
    engine mints per-fork uuid7 ids through the seam: 20 forks → 200
    distinct children, each with its own (parent_id, map_index)."""
    flow_id = await seed_flow(wf_conn, wf_schema)

    # THE RED — A REAL ENGINE MUTATION: the fork's id mint site swapped
    # for the convicted string-shape convention (ids derived from a
    # {parent}.{child}-style convention with no per-fork discriminator —
    # 10 distinct ids TOTAL): the second fork's children collide with the
    # first's on the PRIMARY KEY — the convicted graph cannot even be
    # built, and a convention WITHOUT the PK would silently land the
    # wrong graph (the same ids, the wrong parents' work). The shipped
    # mint (uuid7 via the seam) forks 20 x 10 distinct children.
    import taskq.workflows._fork as fork_module

    convicted_calls = itertools.count()
    monkeypatch.setattr(
        fork_module,
        "new_uuid",
        lambda: uuid.UUID(int=next(convicted_calls) % 10),  # 10 distinct ids, no fork discriminator
    )
    convicted_fork = ForkSpec(
        children=tuple(
            ChildSpec(step_key="enrich", actor="wf", queue="default", map_index=m)
            for m in range(10)
        ),
        join=JoinSpec(step_key="reduce", actor="wf", queue="default"),
    )
    convicted_parent = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="conv")
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        for _ in range(2):
            await fork_module.insert_fork(
                wf_conn,
                wf_sql,
                flow_id=flow_id,
                parent_id=convicted_parent,
                parent_step_key="conv",
                fork=convicted_fork,
            )
    monkeypatch.undo()
    engine_redlog.red(
        "pin18-id-collision",
        "the fork's id mint swapped for a string-shape convention (no per-fork discriminator)",
        {
            "convicted_distinct_ids": 10,
            "observed": "UniqueViolationError on the second fork (the PK is the only backstop)",
        },
    )

    # THE SHIPPED FORK: each parent's finalize atomically forks 10
    # children; every child id is distinct, wiring lives in (parent_id,
    # map_index).
    forked_parents: list[JobId] = []
    for i in range(20):
        fork_parent = await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"fp{i}")
        forked_parents.append(fork_parent)
        fork = ForkSpec(
            children=tuple(
                ChildSpec(step_key="enrich", actor="wf", queue="default", map_index=m)
                for m in range(10)
            ),
            join=JoinSpec(step_key=f"reduce{i}", actor="wf", queue="default"),
        )
        result = await finalize_node(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            job_id=fork_parent,
            step_key=f"fp{i}",
            worker_id=(await claim_view(wf_conn, wf_schema, fork_parent))[0],
            attempt=1,
            claim_epoch=0,
            outcome="succeeded",
            fork=fork,
        )
        assert result.applied
    rows = await wf_conn.fetch(
        f'SELECT id, parent_id, map_index, step_key FROM "{wf_schema}".jobs '
        "WHERE parent_id = ANY($1::uuid[]) ORDER BY parent_id, map_index",
        forked_parents,
    )
    rows = [r for r in rows if r["step_key"] == "enrich"]
    assert len(rows) == 200, len(rows)
    ids = {r["id"] for r in rows}
    assert len(ids) == 200, "every forked child owns a distinct id"
    for r in rows:
        assert r["parent_id"] in forked_parents
        assert r["map_index"] is not None


# ── Pin 19: FORK-DEBT (the atomic fork makes the debt unrepresentable) ──


@pytest.mark.integration
async def test_pin_19_fork_atomicity(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: Any,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """The split-tx variant — the parent's terminal commits, the fork's
    child INSERTs follow — hit by a kill in the window: a TERMINAL PARENT
    WITH A FORK DEBT (the partial fork, the join it owes never fires). The
    ONE-transaction fork path makes it unrepresentable: a kill at ANY
    statement boundary rolls the whole tx back; the reclaim re-forks."""
    dsn = module_pg_schema.pg_dsn
    admin = await asyncpg.connect(dsn)
    flow_id = await seed_flow(wf_conn, wf_schema)

    # THE SPLIT VARIANT (the red): terminal commit, then the kill before
    # the children land.
    split_parent = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="split")
    victim = await asyncpg.connect(dsn)
    victim_pid = await victim.fetchval("SELECT pg_backend_pid()")
    async with victim.transaction():
        await victim.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
            "WHERE id = $1 AND status = 'running' AND attempt = 1",
            split_parent,
        )
    # The kill lands between the two transactions of the split variant —
    # the children were never inserted.
    await admin.execute(f"SELECT pg_terminate_backend({victim_pid})")
    await victim.close()
    engine_redlog.red(
        "pin19-fork-debt",
        "split-tx fork (terminal commit, child inserts follow) killed in the window",
        {"terminal_parent_with_fork_debt": True, "children": 0},
    )
    state = await node_state(wf_conn, wf_schema, split_parent)
    split_children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1', split_parent
    )
    assert state["status"] == "succeeded" and int(split_children) == 0, (
        "the split variant's dragon (terminal parent, fork debt) must be "
        "observable — the red comparator is broken"
    )

    # THE ATOMIC FORK (the shipped shape): the same kill, mid-fork — the
    # whole tx rolls back; the parent stays running (the reclaim re-forks).
    atomic_parent = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="atomic")
    fork = ForkSpec(
        children=tuple(
            ChildSpec(step_key="c", actor="wf", queue="default", map_index=m) for m in range(5)
        ),
        join=JoinSpec(step_key="r", actor="wf", queue="default"),
    )
    killed = await asyncpg.connect(dsn)
    killed_pid = await killed.fetchval("SELECT pg_backend_pid()")
    # The kill surfaces either as a server error or as the client-side
    # interface error the zombie state produces (the fanout cut #7's
    # InternalClientError: another operation in progress).
    with pytest.raises((asyncpg.exceptions.PostgresError, asyncpg.exceptions.InterfaceError)):
        async with killed.transaction():
            await killed.execute(
                f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = "
                "now() WHERE id = $1 AND status = 'running' AND attempt = 1",
                atomic_parent,
            )
            await admin.execute(f"SELECT pg_terminate_backend({killed_pid})")
            await killed.execute(  # the fork's writes: never committed
                f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, '
                "max_attempts, retry_kind, parent_id, map_index, step_key) "
                f"VALUES ($1, 'wf', 'default', '{{}}', 3, 'transient', $2, 0, 'c')",
                new_uuid(),
                atomic_parent,
            )
    await killed.close()
    state = await node_state(wf_conn, wf_schema, atomic_parent)
    assert state["status"] == "running", "the atomic fork's kill rolls the tx back"
    atomic_children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1 AND '
        'NOT metadata @> \'{"blocking_reason": "join"}\'::jsonb',
        atomic_parent,
    )
    assert int(atomic_children) == 0, "never partial"
    # The reclaim re-forks: the retry lands the WHOLE fork — exactly
    # N children + 1 join, never partial, never double.
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=atomic_parent,
        step_key="atomic",
        worker_id=(await claim_view(wf_conn, wf_schema, atomic_parent))[0],
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
        fork=fork,
    )
    assert result.applied
    # The children count EXCLUDES the join node (its parent_id is the fork
    # parent too; the join identifies itself by its blocking reason).
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1 AND '
        'NOT metadata @> \'{"blocking_reason": "join"}\'::jsonb',
        atomic_parent,
    )
    joins = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1 AND '
        'metadata @> \'{"blocking_reason": "join"}\'::jsonb',
        atomic_parent,
    )
    assert int(children) == 5 and int(joins) == 1
    await admin.close()


# ── Pin 20: OUTBOX-FLUSH (the drain completes the dispatch exactly once) ─


@pytest.mark.integration
async def test_pin_20_outbox_drain_exactly_once(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    engine_redlog: RedLog,
) -> None:
    """A crash between fire-commit and consumer-insert → the drain
    completes the dispatch EXACTLY ONCE (the consumer insert idempotent
    ``ON CONFLICT`` on the consumer step key via the composite arbiter,
    the undelivered flag flipped in the insert's tx). The stranded-join
    state (fired join, no consumer) and the double-insert variant each
    red."""
    flow_id = await seed_flow(wf_conn, wf_schema)

    # The finalize forks one child + a join declaring ONE consumer; the
    # join fires in the same finalize's tx2 (deps 1 → 0) and the
    # consumer's outbox row rides tx2. THE CRASH WINDOW: the worker dies
    # before the drain — the outbox row sits undelivered.
    fork_parent = await seed_running_node(wf_conn, wf_schema, flow_id)
    fork = ForkSpec(
        children=(ChildSpec(step_key="c", actor="wf", queue="default"),),
        join=JoinSpec(
            step_key="join",
            actor="wf",
            queue="default",
            consumers=(ConsumerBinding(step_key="consumer", actor="wf", queue="default"),),
        ),
    )
    # The fork parent's terminal + fork land atomically (the engine's own
    # tx1); the join waits on its one child c.
    child_ids, _join_id = await _fork_with_edges(
        wf_conn, wf_schema, wf_sql, flow_id, fork_parent, fork
    )

    # The child c is claimed (running) and finalizes → the join hits 0 and
    # fires in its tx2; the consumer's outbox row rides the same tx2. The
    # worker then DIES before the drain — the outbox row sits undelivered
    # (the stranded-join state the drain exists to cure).
    child_c = child_ids[0]
    c_worker = new_uuid()
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "
        "locked_by_worker = $2, claim_epoch = 0, lock_expires_at = "
        "now() + interval '90 seconds' WHERE id = $1",
        child_c,
        c_worker,
    )
    result = await finalize_node(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        job_id=child_c,
        step_key="c",
        worker_id=c_worker,
        attempt=1,
        claim_epoch=0,
        outcome="succeeded",
    )
    assert result.applied
    assert result.fired, "the join fired in tx2"
    outbox_rows = await wf_conn.fetch(
        f'SELECT id, join_job_id, flow_id, consumer_step_key FROM "{wf_schema}".wf_outbox '
        "WHERE NOT delivered"
    )
    assert len(outbox_rows) == 1, outbox_rows  # the consumer's outbox row

    # THE STRANDED-JOIN RED (the crash window): before the drain, the
    # fired join's consumers never dispatched — the state the drain exists
    # to prevent.
    stranded = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1',
        result.fired[0].join_job_id,
    )
    engine_redlog.red(
        "pin20-outbox-flush-stranded",
        "crash between fire-commit and consumer-insert (no drain yet)",
        {"fired": True, "consumers_dispatched": int(stranded)},
    )
    assert int(stranded) == 0

    # THE SHIPPED DRAIN: the consumer dispatches exactly once...
    delivered = await drain_outbox(module_pg_pool, wf_sql)
    assert delivered == 1, delivered
    consumers = await wf_conn.fetch(
        f'SELECT id, idempotency_scope, idempotency_key FROM "{wf_schema}".jobs WHERE parent_id = $1',
        result.fired[0].join_job_id,
    )
    assert len(consumers) == 1, len(consumers)
    # THE ARBITER'S KEY — the STATIC ROW's own convention
    # (``step_idempotency_key``: scope=workflow:{flow}, key=wf:{step}):
    # the map-join's downstream is a static row since create (the
    # consumption cure), so the drain's consumer insert for it is the
    # arbiter's CONFLICT — the belt, never a second dispatch.
    assert consumers[0]["idempotency_key"] == "wf:consumer"

    # ...and the DOUBLE-INSERT variant reds: a second drain (the crash
    # re-delivery) must be a no-op on the arbiter. The convicted shape
    # (a drain without ON CONFLICT) would insert the second row.
    await drain_outbox(module_pg_pool, wf_sql)
    consumers = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1',
        result.fired[0].join_job_id,
    )
    assert int(consumers) == 1, "the arbiter dedupes the re-drain"
    delivered_flags = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_outbox WHERE delivered'
    )
    assert int(delivered_flags) == 1
    # Clean the drain's inserted consumer row so later counts stay exact.
    await wf_conn.execute(f'DELETE FROM "{wf_schema}".wf_outbox WHERE delivered')


async def _fork_with_edges(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_sql: WorkflowSql,
    flow_id: JobId,
    fork_parent: JobId,
    fork: ForkSpec,
) -> tuple[list[JobId], JobId | None]:
    """Insert the fork's rows via the engine's OWN tx1 (the atomic fork):
    the child rows + edges + the join node land with the parent's terminal.

    Returns (child job ids, the join's id)."""

    parent_view = await claim_view(wf_conn, wf_schema, fork_parent)
    async with wf_conn.transaction():
        await wf_conn.execute(
            f"UPDATE \"{wf_schema}\".jobs SET status = 'succeeded', finished_at = now() "
            "WHERE id = $1 AND status = 'running' AND attempt = $2 AND claim_epoch = $3",
            fork_parent,
            parent_view[1],
            parent_view[2],
        )
        child_ids, join_id = await insert_fork(
            wf_conn,  # pyright: ignore[reportArgumentType]  # Why: insert_fork takes ConnLike; the raw connection is the same surface.
            wf_sql,
            flow_id=flow_id,
            parent_id=fork_parent,
            parent_step_key="fp",
            fork=fork,
        )
    return child_ids, join_id
