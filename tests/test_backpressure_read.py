"""The backpressure read: ``await client.backpressure(queues) -> BackpressureSnapshot``.

LIB-2 (issue #670). A read-only typed read mirroring the reader-method
precedent (``get_row`` / ``list``), NOT an enqueue kwarg: ``enqueue`` is
generic ``JobHandle[R]`` and no flag may flip the reply type.

The pins here are the honest-verdict contract:

* DEPTH counts exactly what admission counts (pending + scheduled, the
  ``enqueue_max_pending_count`` predicate), one indexed aggregate round
  trip per call.
* CAP is the queue's actor effective ``max_pending`` from the
  ``ActorCapacityCache`` TTL snapshot (stored operator cap; the
  ``@actor`` literal is not visible to this process).
* FAN-OUT: children of a parent fan-out carry an exact ``parent_id``
  stamp; the pending-children count outside the queried queue joins the
  verdict.
* FAIL-OPEN: any unavailable half reads as the explicit ``unknown``
  state with a reason — NEVER a fabricated verdict (the
  ``maybe_warn_unserved_queue`` posture).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from taskq._ids import new_job_id
from taskq.client._enqueuer import _parent_job_id_var, current_parent_id, set_parent_job_id
from taskq.client._jobs import JobsClient
from taskq.testing import FakeClock, InMemoryBackend, make_enqueue_args
from taskq.types import BackpressureSnapshot

if TYPE_CHECKING:
    from taskq.backend._protocol import JobId

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


async def _seed_pending(backend: InMemoryBackend, queue: str, n: int, **kwargs: object) -> None:
    for _ in range(n):
        args = make_enqueue_args(queue=queue, scheduled_at=_NOW - timedelta(seconds=1), **kwargs)  # pyright: ignore[reportArgumentType]
        await backend.enqueue(args)


async def test_read_is_exported_snapshot_type() -> None:
    """The read returns the exported ``BackpressureSnapshot``, one entry per queue."""
    backend = InMemoryBackend(FakeClock(_NOW))
    client = JobsClient(backend)
    snapshot = await client.backpressure(["q1", "q2"])
    assert isinstance(snapshot, BackpressureSnapshot)
    assert set(snapshot.queues) == {"q1", "q2"}


async def test_depth_counts_admission_statuses_exactly() -> None:
    """depth counts pending+scheduled (what the cap counts), not running.

    The verdict predicts the next enqueue's admission: the cap
    (``enqueue_max_pending_count``) counts pending+scheduled only, so a
    queue of RUNNING jobs (holding no pending slot) is not "over".
    """
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=10)
    await _seed_pending(backend, "q", 3)
    # A scheduled (future) job also holds a pending slot.
    args = make_enqueue_args(queue="q", scheduled_at=_NOW + timedelta(hours=1))
    await backend.enqueue(args)
    # A running job does NOT hold a pending slot: dispatch one.
    await backend.enqueue(make_enqueue_args(queue="q"))
    await backend.dispatch_batch(
        backend._worker_id, ["q"], limit=1, lock_lease=timedelta(seconds=60)
    )

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.depth == 4  # 3 pending + 1 scheduled; the running row is not counted


async def test_over_and_ok_boundary_is_depth_lt_cap() -> None:
    """depth >= cap reads 'over'; depth < cap reads 'ok'.

    The flip is driven from the CAP side (a stored-override raise, the
    ``taskq actor-config set`` shape, cache invalidated): the terminal
    writes fence on claimed rows, so draining a pending row is not a
    lever a test can pull.
    """
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=2)
    await _seed_pending(backend, "q", 2)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.state == "over"
    assert entry.effective_max_pending == 2
    assert entry.reason is None

    # The operator raises the cap: below the boundary flips the verdict
    # (the snapshot re-reads at the next TTL window; invalidate is the
    # operator-tooling seam that makes the change visible NOW).
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=5)
    client._capacity_cache.invalidate()
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.state == "ok"
    assert entry.depth == 2
    assert entry.effective_max_pending == 5


async def test_tightest_cap_wins_when_multiple_actors_route() -> None:
    """Several actors route to one queue: the tightest stored cap is the boundary."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="a_wide", queue="q", max_pending=100)
    backend.register_actor_config(actor="b_tight", queue="q", max_pending=2)
    await _seed_pending(backend, "q", 3)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.effective_max_pending == 2
    assert entry.state == "over"


# ── The fail-open posture: unknown, never a fabricated verdict ──────────


class _NoDepthReadBackend(InMemoryBackend):
    """A backend built before the staged depth read existed."""

    count_pending_jobs_by_queue = None  # type: ignore[assignment]


class _NoChildrenReadBackend(InMemoryBackend):
    """A backend built before the staged children read existed."""

    count_pending_children_by_queue = None  # type: ignore[assignment]


class _SickDepthBackend(InMemoryBackend):
    async def count_pending_jobs_by_queue(self, queues: list[str]) -> dict[str, int]:
        raise RuntimeError("connection reset mid-count")


class _SickCapBackend(InMemoryBackend):
    async def get_actor_max_pending(self) -> dict[str, int | None]:
        raise RuntimeError("sick database")


async def test_unknown_when_backend_lacks_the_depth_read() -> None:
    """A capability-less backend (built before the staged read) reads unknown."""
    backend = _NoDepthReadBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=5)
    await _seed_pending(backend, "q", 5)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.state == "unknown"
    assert entry.depth is None
    assert "count_pending_jobs_by_queue" in (entry.reason or "")


async def test_unknown_when_depth_read_fails() -> None:
    """A sick database reads unknown; the read never raises."""
    backend = _SickDepthBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=5)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.state == "unknown"
    assert entry.depth is None
    assert "count_pending_jobs_by_queue" in (entry.reason or "")


async def test_unknown_when_capacity_snapshot_unavailable() -> None:
    """No successful capacity refresh -> no cap -> unknown (never a fabricated ok)."""
    backend = _SickCapBackend(FakeClock(_NOW))
    await _seed_pending(backend, "q", 1)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.state == "unknown"
    assert entry.effective_max_pending is None
    assert entry.reason is not None


async def test_unknown_when_queue_is_unserved() -> None:
    """A queue no stored assignment routes reads unknown, the unserved posture."""
    backend = InMemoryBackend(FakeClock(_NOW))
    await _seed_pending(backend, "stranded", 1)

    client = JobsClient(backend)
    entry = (await client.backpressure(["stranded"])).queues["stranded"]
    assert entry.state == "unknown"
    assert entry.effective_max_pending is None
    assert "assignment" in (entry.reason or "")


async def test_unknown_when_cap_is_the_code_literal_only() -> None:
    """No stored override: the @actor literal is code-side, this process cannot see it."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=None)
    await _seed_pending(backend, "q", 1)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.state == "unknown"
    assert entry.effective_max_pending is None
    assert entry.reason is not None


# ── The fan-out half: exact parent_id accounting ─────────────────────────


async def test_fanout_children_outside_the_queue_join_the_verdict() -> None:
    """Parent in queue ``p``, children pending in ``c``: querying ``p`` counts them."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="p", max_pending=2)
    parent_id = new_job_id()
    await _seed_pending(backend, "p", 0)
    # 1 unrelated pending job in p (depth=1), 3 pending children of the
    # parent in queue c (outside p).
    await backend.enqueue(make_enqueue_args(queue="p"))
    for _ in range(3):
        args = make_enqueue_args(queue="c")
        await backend.enqueue(replace(args, parent_id=parent_id))

    client = JobsClient(backend)
    entry = (await client.backpressure(["p"], parent_id=parent_id)).queues["p"]
    assert entry.children_depth == 3  # the parent's pending children outside p
    assert entry.depth == 1
    assert entry.state == "over"  # 1 + 3 >= cap 2
    assert entry.reason is None


async def test_children_in_the_queried_queue_are_not_double_counted() -> None:
    """Children already in the queue sit in its depth; the fan-out half adds the rest only.

    Every counted job is counted exactly once: depth is the queue's own
    admission load, children_depth is the pending-children inflow that
    could still land here. The union, never the sum-with-overlap.
    """
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="p", max_pending=4)
    parent_id = new_job_id()
    # 3 pending children of the parent IN p, plus 1 unrelated pending row.
    for _ in range(3):
        args = make_enqueue_args(queue="p")
        await backend.enqueue(replace(args, parent_id=parent_id))
    await backend.enqueue(make_enqueue_args(queue="p"))

    client = JobsClient(backend)
    entry = (await client.backpressure(["p"], parent_id=parent_id)).queues["p"]
    assert entry.depth == 4
    assert entry.children_depth == 0  # all children are IN p already
    assert entry.state == "over"  # 4 >= 4, counted once

    # The operator raises the cap: the verdict steps down with it (the
    # depth itself is untouched — the union counted each job once).
    backend.register_actor_config(actor="test_actor", queue="p", max_pending=5)
    client._capacity_cache.invalidate()
    entry = (await client.backpressure(["p"], parent_id=parent_id)).queues["p"]
    assert entry.state == "ok"
    assert entry.depth == 4
    assert entry.children_depth == 0

async def test_no_parent_in_play_children_depth_is_none() -> None:
    """Without a parent, the fan-out half is absent (None), the verdict is depth vs cap."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=1)
    await _seed_pending(backend, "q", 1)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"])).queues["q"]
    assert entry.children_depth is None
    assert entry.state == "over"


async def test_ambient_parent_id_from_the_worker_context_flows() -> None:
    """A caller under the parent's context (worker entry) needs no explicit id."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="p", max_pending=1)
    parent_id = new_job_id()
    args = make_enqueue_args(queue="c")
    await backend.enqueue(replace(args, parent_id=parent_id))

    client = JobsClient(backend)
    token = set_parent_job_id(parent_id)
    try:
        snapshot = await client.backpressure(["p"])
    finally:
        from taskq.client._enqueuer import _parent_job_id_var

        _parent_job_id_var.reset(token)
    assert snapshot.parent_id == parent_id
    assert snapshot.queues["p"].children_depth == 1
    assert snapshot.queues["p"].state == "over"  # 0 depth + 1 child >= cap 1


async def test_unknown_when_parent_children_read_is_missing() -> None:
    """A parent is in play but the backend lacks the children read: unknown, never 'ok'."""
    backend = _NoChildrenReadBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=100)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"], parent_id=new_job_id())).queues["q"]
    assert entry.state == "unknown"
    assert "count_pending_children_by_queue" in (entry.reason or "")


class _SickChildrenBackend(InMemoryBackend):
    async def count_pending_children_by_queue(self, parent_id: JobId) -> dict[str, int]:
        raise RuntimeError("sick database")


async def test_unknown_when_children_read_fails() -> None:
    backend = _SickChildrenBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=100)

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"], parent_id=new_job_id())).queues["q"]
    assert entry.state == "unknown"
    assert "count_pending_children_by_queue" in (entry.reason or "")


async def test_actor_body_read_via_the_sub_job_enqueuer() -> None:
    """``ctx.jobs.backpressure(...)``, the fan-out decision's home: same
    contract, the enqueuer's own backend and cache, ambient parent = THIS job."""
    from taskq.client._enqueuer import SubJobEnqueuer

    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="p", max_pending=1)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),
        backend=backend,
        clock=FakeClock(_NOW),
    )
    parent_id = new_job_id()
    args = make_enqueue_args(queue="c")
    await backend.enqueue(replace(args, parent_id=parent_id))

    token = _parent_job_id_var.set(parent_id)
    try:
        snapshot = await enqueuer.backpressure(["p"])
    finally:
        _parent_job_id_var.reset(token)

    assert snapshot.parent_id == parent_id
    entry = snapshot.queues["p"]
    assert entry.children_depth == 1
    assert entry.state == "over"  # 0 depth + 1 pending child >= cap 1


async def test_dangling_parent_reads_as_defined() -> None:
    """A purged / never-existed parent: the count answers 0, the verdict stays well-formed.

    parent_id is a plain column (NO foreign key, the binding constraint:
    per-child key-share locks would serialize the hottest table's inserts
    and retention purges must never block on pending children). A parent
    row that is gone while children pend is a DEFINED, harmless state.
    """
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=1)
    await _seed_pending(backend, "q", 1)

    client = JobsClient(backend)
    # A parent id that never existed in this store (fabricated/stale
    # contextvar across workers — a cross-process id is just a UUID here).
    entry = (await client.backpressure(["q"], parent_id=new_job_id())).queues["q"]
    assert entry.children_depth == 0
    assert entry.state == "over"  # depth 1 >= cap 1, children contributed nothing


# ── The three dangling-parent cases (the oban-pro lesson: missing parents
#    are first-class STATES, never errors; we PRICE the ambiguity instead
#    of mopping it up with constraints — nothing in the DB distinguishes
#    these three, and the snapshot deliberately does NOT try) ────────────


async def test_dangling_parent_never_existed_reads_well_formed() -> None:
    """Case 1 — parent NEVER EXISTED (a fabricated/stale id): the count counts
    children by parent_id regardless; the verdict is a well-formed ok/over."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=100)
    phantom = new_job_id()  # no row will ever carry this id as its own
    child = make_enqueue_args(queue="c")  # the child lands in a DIFFERENT queue
    await backend.enqueue(replace(child, parent_id=phantom))

    client = JobsClient(backend)
    entry = (await client.backpressure(["q", "c"], parent_id=phantom)).queues["q"]
    # The child sits in queue c (outside q): q's fan-out half counts it,
    # counted by parent_id regardless of the parent's (non-)existence.
    assert entry.children_depth == 1
    assert entry.state == "ok"  # 0 + 1 < 100, well-formed, no special case
    assert (await client.backpressure(["q", "c"], parent_id=phantom)).queues["c"].depth == 1


async def test_dangling_parent_purged_by_retention_reads_well_formed() -> None:
    """Case 2 — parent PURGED by retention (dangling, children pending): same verdict.

    The count never joins to the parent row; whether the parent's row
    exists is invisible to the verdict, which is the honest answer for an
    advisory signal: admission pressure is a property of the CHILDREN.
    """
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=100)
    parent = make_enqueue_args(queue="q", scheduled_at=_NOW - timedelta(seconds=1))
    parent_row = await backend.enqueue(parent)
    child = make_enqueue_args(queue="c", scheduled_at=_NOW - timedelta(seconds=1))
    await backend.enqueue(replace(child, parent_id=parent_row.id))  # the child lands in a DIFFERENT queue

    # The parent goes terminal and the retention sweep archives it,
    # children pending. The terminal write fences on a claimed row, so
    # the parent is dispatched (claimed) first.
    await backend.dispatch_batch(
        backend._worker_id, ["q"], limit=1, lock_lease=timedelta(seconds=60)
    )
    await backend.mark_succeeded(parent_row.id, backend._worker_id, attempt=1, claim_epoch=1)
    backend.advance_clock_to(_NOW + timedelta(hours=1))  # the finished_at must age past the cutoff
    backend.archive_terminal_jobs(retention=timedelta(0), archive_retention=timedelta(days=365))
    hot_row = await backend.get(parent_row.id)
    assert hot_row is not None and hot_row.archived  # moved out of jobs, read from the archive tier

    client = JobsClient(backend)
    entry = (await client.backpressure(["q", "c"], parent_id=parent_row.id)).queues["q"]
    assert entry.children_depth == 1  # the pending child: counted by parent_id, parent gone
    assert entry.state == "ok"  # 0 + 1 < 100, well-formed, no special case


async def test_dangling_parent_not_yet_inserted_reads_well_formed() -> None:
    """Case 3 — parent NOT YET INSERTED (children stamped before the parent's
    row commits): the count counts what is there NOW, honestly, as of the
    read's instant; no waiting, no error, a well-formed verdict."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=100)
    future_parent = new_job_id()  # its row commits AFTER this read

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"], parent_id=future_parent)).queues["q"]
    assert entry.children_depth == 0
    assert entry.depth == 0
    assert entry.state == "ok"


async def test_snapshot_carries_its_own_as_of_story() -> None:
    """The prefect lesson: a typed snapshot that does not say how old it is
    would be a lie by omission. ``as_of`` is the read instant; the cap half
    states its own age, bounded by the cache TTL."""
    from datetime import UTC, datetime

    backend = InMemoryBackend(FakeClock(_NOW))
    client = JobsClient(backend)

    before = datetime.now(UTC)
    snapshot = await client.backpressure(["q"])
    after = datetime.now(UTC)

    assert snapshot.as_of.tzinfo is UTC
    assert before <= snapshot.as_of <= after
    # No refresh has succeeded yet on this client: the cap half's age is
    # None, and the honesty travels WITH the snapshot.
    assert snapshot.cap_age_seconds is None or snapshot.cap_age_seconds >= 0.0


async def test_snapshot_cap_age_bounded_by_the_ttl() -> None:
    """A live snapshot's cap age is the monotonic delta since its refresh —
    small, non-negative, and the verdict carries it."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=1)
    client = JobsClient(backend)
    await client._capacity_cache.refresh()

    snapshot = await client.backpressure(["q"])
    assert snapshot.cap_age_seconds is not None
    assert 0.0 <= snapshot.cap_age_seconds < 5.0  # DEFAULT_CAPACITY_CACHE_TTL


async def test_retention_sweep_never_touches_pending_children() -> None:
    """The oban-pro preserve_workflows lesson: retention must be linkage-aware
    or backpressure numbers lie. TaskQ's purge is TERMINAL-only (the archive
    candidates are finished_at-bounded terminal rows), so pending children of
    any parent — purged or live — are already excluded BY CONSTRUCTION: the
    sweep cannot strand them, and the count survives the sweep unchanged."""
    backend = InMemoryBackend(FakeClock(_NOW))
    backend.register_actor_config(actor="test_actor", queue="q", max_pending=100)
    parent = make_enqueue_args(queue="q", scheduled_at=_NOW - timedelta(seconds=1))
    parent_row = await backend.enqueue(parent)
    for _ in range(3):
        child = make_enqueue_args(queue="q", scheduled_at=_NOW - timedelta(seconds=1))
        await backend.enqueue(replace(child, parent_id=parent_row.id))

    # The parent finishes and ages out; the children are still pending.
    # The terminal write fences on a claimed row, so the parent is
    # dispatched (claimed) first.
    await backend.dispatch_batch(
        backend._worker_id, ["q"], limit=1, lock_lease=timedelta(seconds=60)
    )
    await backend.mark_succeeded(parent_row.id, backend._worker_id, attempt=1, claim_epoch=1)
    backend.advance_clock_to(_NOW + timedelta(hours=1))  # the finished_at must age past the cutoff
    result = backend.archive_terminal_jobs(
        retention=timedelta(0), archive_retention=timedelta(days=365)
    )
    assert result.by_status.get("succeeded", 0) == 1  # the parent moved to the archive

    client = JobsClient(backend)
    entry = (await client.backpressure(["q"], parent_id=parent_row.id)).queues["q"]
    assert entry.depth == 3  # the pending children: untouched by the sweep
    assert entry.children_depth == 0  # all three are IN q, counted once
    assert entry.state == "ok"


async def test_empty_queues_read_is_an_empty_snapshot() -> None:
    """Mirrors count_active_jobs' empty-list contract: zero input, zero output."""
    backend = InMemoryBackend(FakeClock(_NOW))
    client = JobsClient(backend)
    snapshot = await client.backpressure([])
    assert snapshot.queues == {}


def test_current_parent_id_defaults_to_none() -> None:
    """The ambient parent context is unset outside a worker entry."""
    assert current_parent_id() is None
