# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""InMemoryBackend must be observably equivalent to PostgresBackend at the
dispatch seam — same inputs, same rows, same order.

The existing equivalence guards (``test_in_memory_read_isolation`` /
``test_in_memory_seam_registry``) only pin ALIASING and method presence:
they check that a returned row is a fresh object and that every protocol
method exists. Neither observes what ``dispatch_batch`` actually SELECTS.
Three semantic divergences were found that way — all silent, because the
in-memory mirror was the greener of the two — and all three are now fixed
and behaviourally pinned by the tests in this file:

1. An EMPTY ``queues`` list. InMemory filtered with ``not queues or
   row.queue in queues``, so ``[]`` meant "match ALL". PG builds the
   candidate set with ``CROSS JOIN LATERAL unnest(queues)``, and an empty
   array yields zero rows — the CROSS JOIN annihilates every candidate, so
   ``[]`` means "match NOTHING". A suite that dispatched with ``[]`` saw
   work flow in memory while a real worker polls forever claiming nothing.

2. A NULL ``fairness_key`` under ``round_robin``. InMemory synthesised a
   SINGLETON partition per unkeyed job (``f"__null__{r.id}"``), so every
   unkeyed job ranked 1 and crowded to the front of the interleave. PG uses
   ``PARTITION BY COALESCE(j2.fairness_key, '__null__')`` — ONE shared
   partition, ranking 1, 2, 3…, which deliberately de-prioritises the
   unkeyed cohort behind the keyed ones. ``fairness_key`` is None by
   default, so this was the common case, and the in-memory shape was
   exactly the round-robin starvation the mode exists to prevent.

3. ``cancel_where`` returned ids in the mirror's default priority-first
   ``_list_jobs`` ordering while PG returns them UUID-ascending
   (``array_agg(id ORDER BY id)`` over ``ORDER BY id`` windows).

PG is production: these tests assert the PG result and flag InMemory as
the backend that diverged. The dispatch pins drive the SAME job set (same
ids, same ``fairness_key``s, same ``scheduled_at``) through both backends
and compare WHICH jobs a bounded ``dispatch_batch`` claims. The comparison
is on the claimed SET, not on the RETURNING sequence: ``UPDATE … RETURNING``
gives no row order guarantee, so selection — which jobs a limited batch
admits and which it defers — is the observable both backends owe each
other, and it is exactly what each divergence changed. The registry in
``tests/test_backend_semantic_parity_registry.py`` walks the two backends'
shared surface so the next divergence fails on arrival.
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobFilter, JobId, JobRow
from taskq.testing._runner import set_queue_mode
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend

if TYPE_CHECKING:
    import asyncpg

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)

# Every job is enqueued with an explicit past ``scheduled_at`` so neither
# backend's clock domain can decide eligibility: PG compares against
# ``statement_timestamp()`` and InMemory against its own ``FakeClock``,
# and both see this instant as comfortably past.
_SCHEDULED_AT = datetime(2025, 1, 1, tzinfo=UTC)
_IN_MEMORY_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _args(
    *,
    job_id: JobId,
    actor: str,
    queue: str,
    fairness_key: str | None = None,
) -> EnqueueArgs:
    """Identical enqueue input for both backends — the same explicit id is
    the join key the parity comparison is built on."""
    return EnqueueArgs(
        id=job_id,
        actor=actor,
        queue=queue,
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_SCHEDULED_AT,
        fairness_key=fairness_key,
    )


async def _setup_pg_queue(
    conn: "asyncpg.Connection",
    schema: str,
    queue: str,
    mode: str,
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) '
        "ON CONFLICT (name) DO UPDATE SET mode = $2",
        queue,
        mode,
    )


async def _ensure_pg_actor(
    conn: "asyncpg.Connection", schema: str, actor: str, *, queue: str = "default"
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
        "VALUES ($1, NULL, $2, $3::jsonb) "
        "ON CONFLICT (actor) DO UPDATE SET max_concurrent = NULL",
        actor,
        queue,
        "{}",
    )


def _ids(rows: list[JobRow]) -> list[JobId]:
    return [r.id for r in rows]


def _claimed(rows: list[JobRow], names: dict[JobId, str]) -> set[str]:
    """The SET of jobs a bounded dispatch claimed. ``UPDATE … RETURNING``
    has no row-order guarantee, so selection — not sequence — is the
    observable both backends must agree on."""
    return {names.get(r.id, str(r.id)) for r in rows}


async def _make_in_memory(
    args_list: list[EnqueueArgs],
    *,
    round_robin_queues: tuple[str, ...] = (),
) -> InMemoryBackend:
    backend = InMemoryBackend(clock=FakeClock(_IN_MEMORY_NOW))
    # Dispatch candidates come FROM the actor_config registry on BOTH
    # backends (PG's per_actor_capacity CTE — every caller of this helper
    # seeds the PG side's actor_config via _ensure_pg_actor): the twin's
    # registry must carry the same actors, or "no actors registered" would
    # read as "no filter" — the exact mirror-greener-than-production shape
    # the dispatch-parity pins police.
    for actor in {args.actor for args in args_list}:
        backend.register_actor_config(actor=actor)
    for queue in round_robin_queues:
        set_queue_mode(backend, queue, "round_robin")
    for args in args_list:
        await backend.enqueue(args)
    return backend


# ── Divergence 1: an empty ``queues`` list ────────────────────────────


async def test_empty_queues_list_dispatches_identically_in_both_backends(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """``dispatch_batch(queues=[])`` must select the same rows in both
    backends.

    PG's ``CROSS JOIN LATERAL unnest((SELECT queues FROM params))``
    produces zero rows for an empty array, annihilating the candidate set:
    an empty queue list claims NOTHING. InMemory's ``not queues or
    row.queue in queues`` reads the same input as "no filter" and claims
    EVERYTHING — so the mirror dispatches work a real worker never would.
    """
    schema = module_pg_schema.schema_name
    pg_backend = clean_jobs_app.backend
    actor = "parity_empty_queues"

    args_list = [_args(job_id=new_job_id(), actor=actor, queue="default") for _ in range(3)]

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await _setup_pg_queue(conn, schema, "default", "strict_fifo")
        await _ensure_pg_actor(conn, schema, actor)

    for args in args_list:
        await pg_backend.enqueue(args)
    mem_backend = await _make_in_memory(args_list)

    pg_rows = await pg_backend.dispatch_batch(new_uuid(), [], 10, _LEASE)
    mem_rows = await mem_backend.dispatch_batch(new_uuid(), [], 10, _LEASE)

    assert _ids(pg_rows) == [], (
        "PostgresBackend (production) diverged from its own documented "
        f"semantics: an empty queues list should annihilate the candidate "
        f"set via CROSS JOIN LATERAL unnest, but it dispatched {len(pg_rows)} row(s)."
    )
    assert sorted(_ids(mem_rows)) == sorted(_ids(pg_rows)), (
        "InMemoryBackend diverged from PostgresBackend at dispatch_batch("
        f"queues=[]): PG dispatched {len(pg_rows)} row(s) and InMemory "
        f"dispatched {len(mem_rows)}. PG treats an empty queue list as "
        "MATCH NOTHING (CROSS JOIN LATERAL unnest of an empty array yields "
        "zero rows, src/taskq/backend/_dispatch_sql.py:100); InMemory treats "
        "it as MATCH ALL (`not queues or row.queue in queues`, "
        "src/taskq/testing/_dispatch.py:44). The mirror is greener than "
        "production: tests see jobs flow while a real worker claims nothing."
    )


# ── Divergence 2: NULL fairness_key under round_robin ─────────────────


async def test_null_fairness_key_round_robin_selection_matches_pg(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Unkeyed (NULL ``fairness_key``) jobs must share ONE partition in
    both backends, so they rank 1, 2, 3… behind the keyed cohorts.

    Job set for a single actor on one ``round_robin`` queue: three unkeyed
    jobs with the oldest ``scheduled_at``, then one job each for keys "a"
    and "b". PG's ``PARTITION BY COALESCE(fairness_key, '__null__')`` gives
    fairness_rank 1, 2, 3 to the unkeyed cohort, so a ``limit=3`` batch
    admits one job per cohort — null-1, key-a, key-b — and defers null-2 /
    null-3 to a later round. InMemory's per-job synthetic partition
    (``f"__null__{r.id}"``) ranks ALL THREE unkeyed jobs at 1, so the same
    bounded batch is consumed entirely by the unkeyed cohort and the keyed
    cohorts get nothing: round-robin starvation of exactly the kind the
    mode exists to prevent, and ``fairness_key`` is None by default.
    """
    schema = module_pg_schema.schema_name
    pg_backend = clean_jobs_app.backend
    actor = "parity_null_fk"
    queue = "rr_parity"

    names: dict[JobId, str] = {}
    args_list: list[EnqueueArgs] = []

    def _add(name: str, fairness_key: str | None, offset_seconds: int) -> None:
        job_id = new_job_id()
        names[job_id] = name
        args_list.append(
            EnqueueArgs(
                id=job_id,
                actor=actor,
                queue=queue,
                payload={},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=_SCHEDULED_AT + timedelta(seconds=offset_seconds),
                fairness_key=fairness_key,
            )
        )

    # Distinct scheduled_at values make the intended order total and
    # id-independent, so the comparison pins semantics, not UUID luck.
    _add("null-1", None, 0)
    _add("null-2", None, 1)
    _add("null-3", None, 2)
    _add("key-a", "a", 3)
    _add("key-b", "b", 4)

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await _setup_pg_queue(conn, schema, queue, "round_robin")
        await _ensure_pg_actor(conn, schema, actor)

    for args in args_list:
        await pg_backend.enqueue(args)
    mem_backend = await _make_in_memory(args_list, round_robin_queues=(queue,))

    # limit=3 is the discriminating window: exactly one slot per cohort if
    # the '__null__' partition is shared, all three to the unkeyed cohort
    # if it is not.
    pg_rows = await pg_backend.dispatch_batch(new_uuid(), [queue], 3, _LEASE)
    mem_rows = await mem_backend.dispatch_batch(new_uuid(), [queue], 3, _LEASE)

    pg_claimed = _claimed(pg_rows, names)
    mem_claimed = _claimed(mem_rows, names)

    # PG is the production oracle: one shared '__null__' partition means
    # only null-1 holds fairness_rank 1, alongside key-a and key-b.
    assert pg_claimed == {"null-1", "key-a", "key-b"}, (
        "PostgresBackend (production) did not produce the documented "
        "COALESCE(fairness_key, '__null__') single-partition selection; "
        f"claimed {sorted(pg_claimed)}. The parity oracle itself is wrong — "
        "re-derive it before trusting the InMemory comparison below."
    )
    assert mem_claimed == pg_claimed, (
        "InMemoryBackend diverged from PostgresBackend at the round-robin "
        f"fairness seam: with limit=3 PG claimed {sorted(pg_claimed)} and "
        f"InMemory claimed {sorted(mem_claimed)}. PG puts every NULL "
        "fairness_key job in ONE shared partition (PARTITION BY "
        "COALESCE(j2.fairness_key, '__null__'), "
        "src/taskq/backend/_dispatch_sql.py:221) so unkeyed jobs rank "
        "1, 2, 3… and yield their surplus slots to the keyed cohorts; "
        "InMemory gives each unkeyed job its OWN singleton partition "
        '(f"__null__{r.id}", src/taskq/testing/_dispatch.py:60) so every '
        "unkeyed job ranks 1 and consumes the whole bounded batch. "
        "fairness_key is None by default, so the mirror starves keyed "
        "cohorts in the most common configuration there is."
    )


# ── Divergence 3: cancel_where id ordering ────────────────────────────


def _anti_correlated_pending_pair() -> tuple[JobId, JobId]:
    """A ``(low-priority id, high-priority id)`` pair with low < high.

    UUIDv7 ids are time-ordered, so a freshly drawn pair arrives in this
    order almost always; drawing until it does turns the relation into a
    construction guarantee rather than UUID luck. The caller asserts it
    as the precondition the whole pin rests on.
    """
    for _ in range(1000):
        low_id, high_id = new_job_id(), new_job_id()
        if low_id < high_id:
            return low_id, high_id
    raise AssertionError("no id pair with low < high after 1000 draws")


async def test_cancel_where_id_ordering_matches_pg(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """``cancel_where`` must return ``cancelled_ids`` in the same order on
    both backends: job-id ascending.

    Unlike ``dispatch_batch`` — whose ``UPDATE … RETURNING`` carries no
    row-order guarantee, which is why the two pins above compare claimed
    SETS — the PG bulk cancel returns ids from ``array_agg(id ORDER BY
    id)`` over driving windows that are themselves ``ORDER BY id``
    (src/taskq/backend/_cancel_bulk.py:206), so the tuple's ORDER is the
    contract. InMemory delegates to ``_list_jobs(order_by=None)``, whose
    default ordering is ``priority DESC, scheduled_at, id`` — so whenever
    priority order and id order disagree, the mirror returns the same ids
    in a different order.

    The seed makes them disagree by construction: job B (priority 1)
    holds the smaller UUID and job A (priority 9) the larger, so the
    default ordering returns (A, B) while the id ordering returns (B, A).
    """
    schema = module_pg_schema.schema_name
    pg_backend = clean_jobs_app.backend
    actor = "parity_cancel_order"

    b_id, a_id = _anti_correlated_pending_pair()
    # Precondition: priority order (A first, 9 > 1) and id order (B
    # first) disagree — without it both backends return equal tuples and
    # the pin proves nothing.
    assert b_id < a_id

    args_list = [
        EnqueueArgs(
            id=b_id,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_SCHEDULED_AT,
            priority=1,
        ),
        EnqueueArgs(
            id=a_id,
            actor=actor,
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_SCHEDULED_AT,
            priority=9,
        ),
    ]

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await _setup_pg_queue(conn, schema, "default", "strict_fifo")
        await _ensure_pg_actor(conn, schema, actor)

    for args in args_list:
        await pg_backend.enqueue(args)
    mem_backend = await _make_in_memory(args_list)

    pg_result = await pg_backend.cancel_where(JobFilter(actor=actor), reason="parity")
    mem_result = await mem_backend.cancel_where(JobFilter(actor=actor), reason="parity")

    # PG is the production oracle: both seeded jobs come back, smaller id
    # first — the UUID-ascending order array_agg(id ORDER BY id) owes.
    assert pg_result.cancelled_ids == (b_id, a_id), (
        "PostgresBackend (production) did not produce the documented "
        "array_agg(id ORDER BY id) UUID-ascending cancel result: expected "
        f"({b_id}, {a_id}), got {list(pg_result.cancelled_ids)}. The "
        "parity oracle itself is wrong — re-derive it before trusting "
        "the InMemory comparison below."
    )
    assert mem_result.cancelled_ids == pg_result.cancelled_ids, (
        "InMemoryBackend diverged from PostgresBackend at the cancel_where "
        f"id-order seam: PG returned {list(pg_result.cancelled_ids)} "
        "(UUID-ascending — array_agg(id ORDER BY id) over ORDER BY id "
        "windows, src/taskq/backend/_cancel_bulk.py:206) and InMemory "
        f"returned {list(mem_result.cancelled_ids)} (the default "
        "priority-first _list_jobs ordering, src/taskq/testing/"
        "_cancel_bulk.py). The same ids in a different order is a "
        "different answer: any caller correlating cancelled_ids against "
        "its own bookkeeping sees the mirror agree with production only "
        "when the sort keys happen to coincide."
    )


# ── The assignment-routing discriminator ─────────────────────────────


async def test_handback_of_a_never_claimed_row_switches_to_assignment_routing_on_both_backends(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A re-pended row must switch from label routing to assignment routing
    on BOTH backends — and the switch must be visible even for a row that
    was never claimed.

    The row below is cancelled while pending and handed back by an operator
    retry, so ``started_at`` stays NULL forever: a discriminator built on
    the started_at proxy still reads it as producer-placed and routes it by
    its (stale) label — the exact strand the ``assignment_routed`` marker
    exists to close. The observable contract, on each backend: before the
    hand-back a consumer of the actor's assignment queue claims nothing
    (producer placement governs the never-claimed row); after it, a
    consumer of the row's label queue claims nothing and a consumer of the
    actor's current assignment claims the row.
    """
    schema = module_pg_schema.schema_name
    pg_backend = clean_jobs_app.backend
    actor = "parity_repend_switch"
    label_queue = "parity_label_q"
    assignment_queue = "parity_assignment_q"

    job_id = new_job_id()
    args = _args(job_id=job_id, actor=actor, queue=label_queue)

    async with clean_jobs_app.deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await _setup_pg_queue(conn, schema, label_queue, "strict_fifo")
        await _setup_pg_queue(conn, schema, assignment_queue, "strict_fifo")
        await _ensure_pg_actor(conn, schema, actor, queue=assignment_queue)

    await pg_backend.enqueue(args)
    mem_backend = InMemoryBackend(clock=FakeClock(_IN_MEMORY_NOW))
    mem_backend.register_actor_config(actor=actor, queue=assignment_queue)
    await mem_backend.enqueue(args)

    # Producer placement governs while the row has never been handed back:
    # the assignment queue's consumer must NOT claim the labelled row (a
    # router that ignored the label would claim it here).
    pg_before = await pg_backend.dispatch_batch(new_uuid(), [assignment_queue], 10, _LEASE)
    mem_before = await mem_backend.dispatch_batch(new_uuid(), [assignment_queue], 10, _LEASE)
    assert pg_before == [], (
        "PostgresBackend (production) claimed a producer-placed row via the "
        f"actor's assignment queue before any re-pend: {_ids(pg_before)}. "
        "The parity oracle itself is wrong — re-derive it before trusting "
        "the InMemory comparison."
    )
    assert _ids(mem_before) == _ids(pg_before), (
        "InMemoryBackend diverged before the hand-back: a producer-placed row "
        "must route by its own label, so the assignment queue's consumer "
        f"claims nothing; PG claimed {_ids(pg_before)}, InMemory claimed "
        f"{_ids(mem_before)}."
    )

    # The hand-back: cancelled while pending, then re-pended by an operator
    # retry. started_at stays NULL on both backends — only the marker can
    # tell this row's origin apart from a producer placement.
    pg_cancel = await pg_backend.cancel_where(JobFilter(actor=actor), reason="parity")
    mem_cancel = await mem_backend.cancel_where(JobFilter(actor=actor), reason="parity")
    assert pg_cancel.cancelled_ids == (job_id,)
    assert mem_cancel.cancelled_ids == pg_cancel.cancelled_ids
    assert await pg_backend.retry_job(job_id) is True
    assert await mem_backend.retry_job(job_id) is True

    # After the hand-back the label no longer routes the row…
    pg_by_label = await pg_backend.dispatch_batch(new_uuid(), [label_queue], 10, _LEASE)
    mem_by_label = await mem_backend.dispatch_batch(new_uuid(), [label_queue], 10, _LEASE)
    assert pg_by_label == [], (
        "PostgresBackend (production) routed a re-pended row by its stale "
        f"label: {_ids(pg_by_label)}. The parity oracle itself is wrong — "
        "re-derive it before trusting the InMemory comparison."
    )
    assert _ids(mem_by_label) == _ids(pg_by_label), (
        "InMemoryBackend diverged at the discriminator: a re-pended row must "
        "NOT be claimable by its label queue's consumers — PG claimed "
        f"{_ids(pg_by_label)} on the label queue, InMemory claimed "
        f"{_ids(mem_by_label)}. This row was never claimed, so started_at "
        "is NULL: a started_at-proxy discriminator routes it by the label "
        "here, which is exactly the strand the assignment_routed marker "
        "exists to close."
    )

    # …and the actor's current assignment does.
    pg_by_assignment = await pg_backend.dispatch_batch(new_uuid(), [assignment_queue], 10, _LEASE)
    mem_by_assignment = await mem_backend.dispatch_batch(new_uuid(), [assignment_queue], 10, _LEASE)
    assert _ids(pg_by_assignment) == [job_id], (
        "PostgresBackend (production) must route the re-pended row to the "
        f"actor's current assignment queue; claimed {_ids(pg_by_assignment)}. "
        "The parity oracle itself is wrong — re-derive it before trusting "
        "the InMemory comparison."
    )
    assert _ids(mem_by_assignment) == _ids(pg_by_assignment), (
        "InMemoryBackend diverged at the assignment-routed arm: the re-pended "
        f"row must be claimable by the assignment queue's consumers — PG "
        f"claimed {_ids(pg_by_assignment)}, InMemory claimed "
        f"{_ids(mem_by_assignment)}."
    )
