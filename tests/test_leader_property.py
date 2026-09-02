"""Property tests for leader invariants ().

Single-leader invariant — across N pods with random startup/shutdown
  events, exactly 0 or 1 leader at a time.
isolate_self vs sweep_expired_locks conditioned equivalence.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from taskq._ids import new_base62, new_uuid
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps

# ── Single-leader invariant ────────────────────────────────────────


@st.composite
def pod_event_sequences(draw: st.DrawFn) -> tuple[int, list[tuple[int, str, float]]]:
    n_pods = draw(st.integers(min_value=1, max_value=5))
    n_events = draw(st.integers(min_value=1, max_value=15))
    events = draw(
        st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=n_pods - 1),
                st.sampled_from(["start", "stop"]),
                st.floats(min_value=0.0, max_value=10.0),
            ),
            min_size=n_events,
            max_size=n_events,
        )
    )
    return n_pods, events


def _worker_settings(**overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}
    for key, value in overrides.items():
        data[f"TASKQ_{key}"] = value
    return WorkerSettings.load_from_dict(data, validate=False)


def _make_deps() -> WorkerDeps:
    settings = _worker_settings(
        HEARTBEAT_INTERVAL="0.5",
        LOCK_LEASE="2.0",
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )

    class _FakePool:
        @asynccontextmanager
        async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[object, None]:  # noqa: ASYNC109 # Why: mirrors asyncpg.Pool.acquire signature for test doubles.
            yield object()

    return WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type] # Why: FakePool stands in for asyncpg.Pool in lock-simulator test.
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


class _Pod:
    """Synthetic pod for models worker start/stop and lock acquisition."""

    def __init__(self, deps: WorkerDeps, worker_id: UUID) -> None:
        self.deps = deps
        self.worker_id = worker_id
        self._running = False

    @property
    def is_leader(self) -> bool:
        return self.deps.is_leader.is_set()

    @property
    def running(self) -> bool:
        return self._running

    def mark_active(self) -> None:
        self._running = True

    def mark_stopped(self) -> None:
        self._running = False
        self.deps.is_leader.clear()


def _settle(pods: list[_Pod], lock_simulator: dict[str, UUID]) -> None:
    """Let each running non-leader pod attempt to acquire the shared lock.

    The first pod that tries when the lock is free becomes leader.
    Once a leader exists, no other pod can acquire the lock.
    """
    for pod in pods:
        if not pod.running or pod.is_leader:
            continue
        holder = lock_simulator.get("leader")
        if holder is None:
            lock_simulator["leader"] = pod.worker_id
            pod.deps.is_leader.set()


def _release_lock(pod: _Pod, lock_simulator: dict[str, UUID]) -> None:
    if pod.is_leader and lock_simulator.get("leader") == pod.worker_id:
        lock_simulator.pop("leader", None)


@settings(deadline=None, max_examples=50)
@given(event_seq=pod_event_sequences())
@example(event_seq=(1, [(0, "start", 1.0)]))
@example(event_seq=(2, [(0, "start", 0.0), (1, "start", 1.0)]))
@example(event_seq=(2, [(0, "start", 0.0), (0, "stop", 1.0)]))
@example(event_seq=(2, [(0, "start", 0.0), (1, "start", 0.0), (0, "stop", 0.1)]))
@example(event_seq=(3, [(0, "start", 0.0), (1, "start", 0.0), (2, "start", 0.0)]))
async def test_property_single_leader_invariant(
    event_seq: tuple[int, list[tuple[int, str, float]]],
) -> None:
    """Across N pods with random startup/shutdown events, the number of
    pods with is_leader.is_set() is always ≤ 1.

    Uses a shared in-memory advisory-lock simulator. Each "start" event
    adds a pod to the active pool; each "stop" removes it and releases the
    lock. After settlement, at most one pod holds the lock.
    """
    n_pods, events = event_seq
    lock_simulator: dict[str, UUID] = {}
    pods: list[_Pod] = []

    for _pod_idx in range(n_pods):
        pods.append(_Pod(_make_deps(), new_uuid()))

    for evt_idx, (pod_id, action, delay) in enumerate(events):
        pod = pods[pod_id]
        if action == "start":
            pod.mark_active()
            _settle(pods, lock_simulator)
        elif action == "stop":
            _release_lock(pod, lock_simulator)
            pod.mark_stopped()

        leader_count = sum(1 for p in pods if p.is_leader)
        assert leader_count <= 1, (
            f"after event #{evt_idx} ({action} pod {pod_id}), "
            f"leader_count={leader_count}, exceeded 1"
        )

        if delay > 0:
            await asyncio.sleep(min(delay, 0.01))

        _settle(pods, lock_simulator)


def _scheduled_at_match(row_a: object, row_b: object) -> bool:
    """Compare scheduled_at with tolerance for pending-status rows.

    When a job transitions to 'pending', both isolate_self and
    sweep_expired_locks compute ``clock_timestamp() + 5s`` in two separate
    statements, so the comparison window is the cross-statement window (see
    _CROSS_STATEMENT_WINDOW). For crashed rows, ``scheduled_at`` is
    unchanged, so an exact match is expected.
    """
    sa: datetime | None = row_a["scheduled_at"]  # type: ignore[index] # Why: asyncpg.Record supports dict-style access but pyright doesn't see it.
    sb: datetime | None = row_b["scheduled_at"]  # type: ignore[index] # Why: same — asyncpg.Record dict-like access.
    status: str = row_a["status"]  # type: ignore[index] # Why: same.
    if status == "pending":
        return sa is not None and sb is not None and abs(sa - sb) < _CROSS_STATEMENT_WINDOW
    return bool(sa == sb)


# ── Sweep equivalence (PG, integration) ────────────────────────────


_sweep_tuple_strategy = st.tuples(
    st.integers(min_value=0, max_value=10),
    st.integers(min_value=1, max_value=10),
    st.sampled_from(["transient", "indefinite", "non_retryable"]),
    st.integers(min_value=0, max_value=2),
    st.integers(min_value=-3600, max_value=-1),
).filter(lambda t: t[0] < t[1])

# Margin folded into the deep-expiry boundary on both sides when classifying
# a draw. It absorbs app-to-database clock skew — the session pg_container
# fixture reports divergence beyond 0.25s, so 1s is headroom — keeping the
# deterministic classification bands clear of the sweep's own decision
# boundary (see test_property_sweep_equivalence).
_CLASSIFY_SKEW_MARGIN = timedelta(seconds=1)

# The sweep's fixed safety margin on top of the grace periods before an
# in-flight-cancel job becomes crash-reclaim eligible (see the
# _SWEEP_1_SQL carve-out comment in backend/_sweeps.py).
_SWEEP_CANCEL_SAFETY = timedelta(seconds=60)

# Wall-time window for comparing two server-stamped values written by
# separate statements (isolate_self's stamp vs the sweep's stamp). Under a
# parallel -n run contending on the shared PG container, the time between
# those statements is seconds, not milliseconds; the contract is "stamped by
# the same server clock in the same epoch", not "within 5s".
_CROSS_STATEMENT_WINDOW = timedelta(seconds=60)


def _deep_expired(
    lock_expires_at: datetime,
    now: datetime,
    cancel_grace: timedelta,
    cleanup_grace: timedelta,
) -> bool:
    """Whether *lock_expires_at* is past the sweep's deep-expiry boundary
    (``now - cancel_grace - cleanup_grace - 60s``), i.e. a running job with
    an in-flight cancel (cancel_phase != 0) has become crash-reclaim
    eligible for the sweep."""
    return lock_expires_at < now - cancel_grace - cleanup_grace - _SWEEP_CANCEL_SAFETY


def _pinned(
    attempt: int,
    max_attempts: int,
    retry_kind: str,
    cancel_phase: int,
    offset: int,
) -> tuple[int, int, str, int, int]:
    return (attempt, max_attempts, retry_kind, cancel_phase, offset)


def _build_job_insert_sql(schema: str) -> str:
    return (
        f'INSERT INTO "{schema}".jobs ('  # noqa: S608 # Why: schema validated by _open_pg_backend/migrate against _IDENT_RE; test-only, same pattern as test_heartbeat.py.
        "id, actor, queue, payload, max_attempts, retry_kind, "
        "status, priority, attempt, scheduled_at, "
        "locked_by_worker, lock_expires_at, started_at, last_heartbeat_at, "
        "cancel_phase"
        ") VALUES ("
        "$1, $2, $3, $4::jsonb, $5, $6, "
        "'running', 0, $7, $8, "
        "$9, $10, $8, $8, "
        "$11"
        ")"
    )


def _build_select_sql(schema: str) -> str:
    columns = "status, locked_by_worker, lock_expires_at, scheduled_at, finished_at"
    return f'SELECT {columns} FROM "{schema}".jobs WHERE id = $1'  # noqa: S608 # Why: schema validated by _open_pg_backend/migrate against _IDENT_RE; test-only pattern matching test_heartbeat.py.


@pytest.mark.integration
@settings(deadline=None, max_examples=30)
@given(params=_sweep_tuple_strategy)
@example(params=_pinned(1, 3, "transient", 0, -30))
@example(params=_pinned(1, 3, "transient", 1, -30))
@example(params=_pinned(1, 3, "transient", 1, -200))
@example(params=_pinned(2, 3, "transient", 2, -30))
async def test_property_sweep_equivalence(
    pg_dsn: str,
    params: tuple[int, int, str, int, int],
) -> None:
    """For random (attempt, max_attempts, retry_kind, cancel_phase,
    lock_expires_at_offset), isolate_self and sweep_expired_locks produce
    equivalent row state in the equivalence domain, divergent state outside it.

    Equivalence domain: cancel_phase == 0 OR the lock is deeply expired
    (lock_expires_at < clock_timestamp() - cancel_grace - cleanup_grace - 60s).

    Divergence domain: cancel_phase > 0 within grace window. isolate_self
    reclaims the job; sweep_expired_locks leaves it unchanged.

    The sweep evaluates its deep-expiry predicate with its own
    ``clock_timestamp()``, which this test cannot observe. Classifying a
    draw from a single post-sweep clock reading couples the classification
    to the sweep-to-read latency: under a parallel ``-n`` run contending on
    the shared PG container, seconds pass between the sweep's decision and
    the reading, and a draw near the boundary flips the classification
    against the sweep's actual (correct) decision — a load flake, not a
    regression. Instead the sweep is bracketed with server-clock readings
    taken immediately before and after it: a lock deep before the sweep
    started is deep for the sweep too (its clock is only later), and a lock
    not deep after the sweep finished was not deep for it either, both
    modulo _CLASSIFY_SKEW_MARGIN. Draws between those two certainties — a
    band as wide as the sweep's own duration plus the margin — are
    genuinely underdetermined from outside, and the product contract allows
    either sweep outcome there, so that band asserts the disjunction of the
    legal end states.
    """
    from taskq.backend.postgres import PostgresBackend
    from taskq.testing.fixtures import _create_worker, _open_pg_backend
    from taskq.worker.heartbeat import _ISOLATE_JOB_SQL_TEMPLATE

    attempt, max_attempts, retry_kind, cancel_phase, offset_secs = params
    stack, deps, _backend = await _open_pg_backend(
        pg_dsn, schema_name=f"tlp_{new_base62()}".lower()
    )
    try:
        schema = deps.settings.schema_name
        cancel_grace = timedelta(seconds=deps.settings.cancellation_grace_period)
        cleanup_grace = timedelta(seconds=deps.settings.cleanup_grace_period)
        now = datetime.now(UTC)
        lock_expires_at = now + timedelta(seconds=offset_secs)

        worker_id_a = new_uuid()
        worker_id_b = new_uuid()
        job_id_a = new_uuid()
        job_id_b = new_uuid()

        async with deps.heartbeat_pool.acquire() as conn:
            for wid in (worker_id_a, worker_id_b):
                await _create_worker(conn, schema, wid)

            job_sql = _build_job_insert_sql(schema)
            select_sql = _build_select_sql(schema)
            scheduled = now - timedelta(seconds=60)
            for job_id, wid in (
                (job_id_a, worker_id_a),
                (job_id_b, worker_id_b),
            ):
                await conn.execute(
                    job_sql,
                    job_id,
                    "test_actor",
                    "default",
                    '{"key":"value"}',
                    max_attempts,
                    retry_kind,
                    attempt,
                    scheduled,
                    wid,
                    lock_expires_at,
                    cancel_phase,
                )

            isolate_sql = _ISOLATE_JOB_SQL_TEMPLATE.format(schema=schema)
            await conn.execute(isolate_sql, job_id_a, worker_id_a)

            pg_now_before = await conn.fetchval("SELECT clock_timestamp()")
            assert isinstance(pg_now_before, datetime)
            await PostgresBackend.sweep_expired_locks(
                conn, cancel_grace, cleanup_grace, schema=schema
            )
            pg_now_after = await conn.fetchval("SELECT clock_timestamp()")
            assert isinstance(pg_now_after, datetime)

            row_a = await conn.fetchrow(select_sql, job_id_a)
            row_b = await conn.fetchrow(select_sql, job_id_b)
            assert row_a is not None
            assert row_b is not None

            # Conservative classification (see docstring): "equivalence"
            # requires deep even at the pre-sweep clock minus the skew
            # margin; "divergence" requires not-deep even at the post-sweep
            # clock plus the margin. Both directions then hold for the
            # sweep's own, unobservable clock with skew absorbed.
            if cancel_phase == 0 or _deep_expired(
                lock_expires_at, pg_now_before - _CLASSIFY_SKEW_MARGIN, cancel_grace, cleanup_grace
            ):
                domain = "equivalence"
            elif not _deep_expired(
                lock_expires_at, pg_now_after + _CLASSIFY_SKEW_MARGIN, cancel_grace, cleanup_grace
            ):
                domain = "divergence"
            else:
                domain = "ambiguous"

            if domain == "equivalence":
                assert row_a["status"] == row_b["status"]
                assert row_a["locked_by_worker"] == row_b["locked_by_worker"]
                assert row_a["lock_expires_at"] == row_b["lock_expires_at"]
                assert _scheduled_at_match(row_a, row_b)
                if row_a["finished_at"] is not None:
                    assert row_b["finished_at"] is not None
                    assert (
                        abs(row_a["finished_at"] - row_b["finished_at"]) < _CROSS_STATEMENT_WINDOW
                    )
            elif domain == "divergence":
                assert row_a["status"] != "running"
                assert row_b["status"] == "running"
                assert row_b["lock_expires_at"] is not None
            else:
                # Boundary band: both sweep outcomes are contract-legal.
                # isolate_self reclaimed its own job unconditionally; the
                # swept twin is either untouched (the divergence outcome) or
                # reclaimed to the same post-state (the equivalence outcome).
                assert row_a["status"] != "running"
                if row_b["status"] == "running":
                    assert row_b["locked_by_worker"] is not None
                else:
                    assert row_a["status"] == row_b["status"]
                    assert row_a["locked_by_worker"] == row_b["locked_by_worker"]
                    assert row_a["lock_expires_at"] == row_b["lock_expires_at"]
                    assert _scheduled_at_match(row_a, row_b)
    finally:
        await stack.aclose()
