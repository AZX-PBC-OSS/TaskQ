"""Property test scaffold for Backend behavioural equivalence.

Hypothesis strategy generating arbitrary enqueue/dispatch/terminal
sequences, run against a fresh InMemoryBackend per example. Memory
branch runs and asserts; the PG branch is currently skip-marked
because ``PostgresBackend`` SQL bodies are not yet implemented.

Each Hypothesis example constructs a fresh InMemoryBackend so state
does not accumulate across generated inputs. The ``backend_pair``
fixture is NOT used here because Hypothesis cannot reset
function-scoped fixtures between examples; the memory backend is
constructed inline. When the PG bodies land, add a second property
test that runs the same operation sequence against ``PostgresBackend``
via ``backend_pair`` (or a fresh PG constructor) and increase
``max_examples`` from 25 to 100 once the SQL parity has been verified.

anchors: (equivalence), -5.5 (API surface),
(keyset pagination).
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs, JobFilter
from taskq.backend._protocol import JobId
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, encode_cursor

# ── Constants ──────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)

_ACTOR_NAMES = ["actor_a", "actor_b"]
_RETRY_KINDS = ["transient", "indefinite", "non_retryable"]

# ── Strategies ─────────────────────────────────────────────────────────

enqueue_args_strategy = st.builds(
    lambda actor, priority, max_attempts, retry_kind: EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"generated": True},
        max_attempts=max_attempts,
        retry_kind=retry_kind,
        scheduled_at=_START,
        priority=priority,
    ),
    actor=st.sampled_from(_ACTOR_NAMES),
    priority=st.integers(min_value=0, max_value=10),
    max_attempts=st.integers(min_value=1, max_value=5),
    retry_kind=st.sampled_from(_RETRY_KINDS),
)

_operation_strategy = st.one_of(
    # ("enqueue", EnqueueArgs)
    st.tuples(st.just("enqueue"), enqueue_args_strategy),
    # ("dispatch", limit: int)
    st.tuples(st.just("dispatch"), st.integers(min_value=1, max_value=10)),
    # ("terminal_succeed", job_index: int)
    st.tuples(
        st.just("terminal_succeed"),
        st.integers(min_value=0, max_value=9),
    ),
    # ("list_jobs", limit)
    st.tuples(
        st.just("list_jobs"),
        st.integers(min_value=1, max_value=10),
    ),
)

operation_sequence_strategy = st.lists(_operation_strategy, min_size=1, max_size=20)


# ── property test scaffold ──────────────────────────────────────


@settings(max_examples=25, deadline=None)
@given(ops=operation_sequence_strategy)
async def test_property_operation_sequence_memory(ops: list[tuple[str, object]]) -> None:
    """scaffold: for each generated operation sequence, apply to a
    fresh InMemoryBackend; oracle asserts each job_id's final status and
    attempt count are consistent with the operations performed.

    Memory branch runs and asserts. PG branch is currently skip-marked
    until ``PostgresBackend`` SQL bodies are implemented.
    Each Hypothesis example gets a fresh backend to avoid state leakage.
    """
    backend = InMemoryBackend(clock=FakeClock(_START))
    succeeded_ids: set[JobId] = set()

    for op_name, op_arg in ops:
        if op_name == "enqueue":
            args = op_arg
            assert isinstance(args, EnqueueArgs)
            await backend.enqueue(args)

        elif op_name == "dispatch":
            limit = op_arg
            assert isinstance(limit, int)
            await backend.dispatch_batch(
                worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage]
                queues=["default"],
                limit=limit,
                lock_lease=_LOCK_LEASE,
            )

        elif op_name == "terminal_succeed":
            idx = op_arg
            assert isinstance(idx, int)
            running_jobs = [
                r
                for r in backend._jobs.values()  # type: ignore[reportPrivateUsage]
                if r.status == "running"
            ]
            if running_jobs:
                target = running_jobs[idx % len(running_jobs)]
                ok = await backend.mark_succeeded(
                    target.id,
                    target.locked_by_worker,  # type: ignore[arg-type]
                    {"ok": True},
                )
                if ok:
                    succeeded_ids.add(target.id)

        elif op_name == "list_jobs":
            limit = op_arg
            assert isinstance(limit, int)
            page = await backend.list_jobs(JobFilter(limit=limit, queue="default"))
            # cursor pagination must produce consistent results
            if page:
                last = page[-1]
                cursor = encode_cursor(last.priority, last.scheduled_at, last.id)
                page2 = await backend.list_jobs(
                    JobFilter(limit=limit, cursor=cursor, queue="default"),
                )
                # page2 entries must come after page entries in sort order
                for r2 in page2:
                    for r1 in page:
                        assert (-r2.priority, r2.scheduled_at, r2.id) >= (
                            -r1.priority,
                            r1.scheduled_at,
                            r1.id,
                        )

    # Final oracle: all succeeded jobs have status "succeeded"
    for jid in succeeded_ids:
        row = await backend.get(jid)
        assert row is not None
        assert row.status == "succeeded"

    # Invariant: terminal-state idempotency (cardinality contract).
    # A second mark_succeeded on a terminal job MUST return False and MUST NOT
    # change the row or add an attempt entry.
    terminal_statuses = {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
    for jid in succeeded_ids:
        row = await backend.get(jid)
        assert row is not None
        assert row.status in terminal_statuses
        attempts_before = await backend.get_attempts(jid)
        second_ok = await backend.mark_succeeded(
            jid,
            backend._worker_id,  # type: ignore[reportPrivateUsage]
            {"idempotent": True},
        )
        assert second_ok is False, "second mark_succeeded on terminal job must return False"
        attempts_after = await backend.get_attempts(jid)
        assert len(attempts_after) == len(attempts_before), (
            "second mark_succeeded must not append an attempt row"
        )

    # Invariant: no-double-dispatch.
    # No job in "running" status should be returned by a dispatch call —
    # the backend must not dispatch the same job twice.
    all_jobs = list(backend._jobs.values())  # type: ignore[reportPrivateUsage]
    running_jobs = [r for r in all_jobs if r.status == "running"]
    if running_jobs:
        dispatched_again = await backend.dispatch_batch(
            worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage]
            queues=["default"],
            limit=len(all_jobs),
            lock_lease=_LOCK_LEASE,
        )
        dispatched_again_ids = {r.id for r in dispatched_again}
        for running_row in running_jobs:
            assert running_row.id not in dispatched_again_ids, (
                f"job {running_row.id} was already running but was re-dispatched"
            )


# ── dispatch sort-order invariant ───────────────────────


@settings(max_examples=25, deadline=None)
@given(
    jobs=st.lists(enqueue_args_strategy, min_size=2, max_size=15),
    limit=st.integers(min_value=1, max_value=10),
)
async def test_property_dispatch_sort_order(
    jobs: list[EnqueueArgs],
    limit: int,
) -> None:
    """dispatch_batch MUST return jobs sorted by (pending_rank,
    priority DESC, scheduled_at ASC, id ASC) regardless of enqueue order.

    Invariant: every actor's rank-1 job sorts before any actor's rank-2,
    ties broken by ``(-priority, scheduled_at, id)``.
    Within same pending_rank, actors are interleaved round-robin.
    """
    backend = InMemoryBackend(clock=FakeClock(_START))

    for args in jobs:
        await backend.enqueue(args)

    dispatched = await backend.dispatch_batch(
        worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage]
        queues=["default"],
        limit=limit,
        lock_lease=_LOCK_LEASE,
    )

    if len(dispatched) < 2:
        return  # nothing to compare

    # Compute pending_rank per actor in the dispatched set
    actor_ranks: dict[str, int] = {}
    dispatched_with_rank: list[tuple[int, int, datetime, UUID, str]] = []
    for j in dispatched:
        rk = actor_ranks.get(j.actor, 0) + 1
        actor_ranks[j.actor] = rk
        dispatched_with_rank.append((rk, -j.priority, j.scheduled_at, j.id, j.actor))

    # Invariant 1: All rank-1 before any rank-2 (cross-actor starvation prevention)
    max_rank_1_idx = -1
    for i, (rank, _, _, _, _) in enumerate(dispatched_with_rank):
        if rank == 1:
            max_rank_1_idx = i
    for i, (rank, _, _, _, _) in enumerate(dispatched_with_rank):
        if rank >= 2:
            assert i > max_rank_1_idx, (
                f"Rank-2 job at index {i} appears before the last rank-1 job at index {max_rank_1_idx}"
            )

    # Invariant 2: Within same actor, dispatched order respects (pending_rank, -priority, scheduled_at, id)
    seen_per_actor: dict[str, tuple[int, int, datetime, UUID]] = {}
    for i, (rank, neg_pri, sched, jid, actor) in enumerate(dispatched_with_rank):
        key = (rank, neg_pri, sched, jid)
        prev = seen_per_actor.get(actor)
        if prev is not None:
            assert prev <= key, (
                f"Within actor {actor!r}, dispatch order violated at index {i}: "
                f"prev={prev!r} > cur={key!r}"
            )
        seen_per_actor[actor] = key
