# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team attacks on the stranded-jobs detector's blind spots.

The detector's entire predicate is ``status IN ('pending','scheduled') AND
NOT EXISTS (actor_config row)`` (src/taskq/worker/_leader_sweeps.py:1228-1236)
— it keys on actor_config presence only. Two permanently-undispatchable
strand shapes are invisible to it:

* (i) a pending row WITH actor_config on a queue no worker serves: dispatch's
  candidates lateral annihilates it (``j2.queue = sq.queue_name``,
  src/taskq/backend/_dispatch_sql.py:229), and with schedule_to_close NULL
  the deadline sweep (sweep 2) cannot fail it either — the row is stranded
  forever while its actor_config row exists.
* (ii) the singleton amplifier: a pending singleton blocker whose
  actor_config was deleted blocks every later enqueue for that actor with
  SingletonCollisionError and retry_after=None (no schedule_to_close → no
  advisory hint; src/taskq/backend/_enqueue.py:550-574) — permanently,
  because the blocker itself is stranded by shape (ii)'s own rule.

The contract: every strand shape the fleet can accumulate must be visible to
the detector (or an equivalent signal).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import Mock
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL
from taskq.backend._dispatch_sql import dispatch_batch as dispatch_batch_sql
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import SingletonCollisionError
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _stranded_jobs_loop

pytestmark = pytest.mark.integration

_SHAPE_I_ACTOR = "orphan_actor_q"
_SHAPE_II_ACTOR = "solo_b"
_NO_WORKER_QUEUE = "no-worker-queue"


class _StubBackendDeps:
    """Duck-typed BackendDeps: settings + pools only (enqueue_with_conn path)."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None


def _pool_free_backend(schema: str) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema,
        },
        validate=False,
    )
    return PostgresBackend(
        _StubBackendDeps(settings),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; enqueue_with_conn reads only settings + the caller's conn.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


class _DetectorDeps:
    """Duck-typed WorkerDeps for _stranded_jobs_loop: settings, worker_pool,
    is_leader, liveness — the only fields the loop body touches."""

    def __init__(self, settings: WorkerSettings, pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = pool
        self.heartbeat_pool = pool
        self.dispatcher_pool = pool
        self.is_leader = asyncio.Event()
        self.is_leader.set()
        self.liveness = Mock()


async def _run_detector(
    pg_dsn: str, pool: asyncpg.Pool, schema: str, monkeypatch: pytest.MonkeyPatch
) -> tuple[dict[str, int], list[dict[str, object]]]:
    """Run the REAL detector loop for a few fast ticks; return the last gauge
    and every log event the loop emitted (the per-shape warnings are the only
    surface the per-condition counts and queue names reach)."""
    import structlog.testing

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_STRANDED_JOBS_INTERVAL": "0.05",
        },
        validate=False,
    )
    ctx = SweepContext(
        deps=_DetectorDeps(settings, pool),  # type: ignore[arg-type]  # Why: duck-typed WorkerDeps carrying exactly the fields the loop reads.
        backend=None,  # type: ignore[arg-type]  # Why: the stranded loop never touches ctx.backend.
        clock=SystemClock(),
        worker_id=new_uuid(),
    )
    published: list[dict[str, int]] = []

    def _capture(data: dict[str, int]) -> None:
        published.append(dict(data))

    monkeypatch.setattr("taskq.worker._leader_sweeps.update_stranded_jobs_cache", _capture)

    shutdown = asyncio.Event()
    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(_stranded_jobs_loop(ctx, shutdown))
        await asyncio.sleep(0.25)
        shutdown.set()
        await asyncio.wait_for(task, timeout=5.0)
    assert published, "detector loop never published a tick"
    return published[-1], [dict(event) for event in captured]


async def _run_stranded_detector_once(
    pg_dsn: str, pool: asyncpg.Pool, schema: str, monkeypatch: pytest.MonkeyPatch
) -> dict[str, int]:
    """Run the REAL detector loop for a few fast ticks; return the last gauge."""
    gauge, _events = await _run_detector(pg_dsn, pool, schema, monkeypatch)
    return gauge


async def _seed_strand_shapes(conn: asyncpg.Connection, schema: str) -> tuple[UUID, UUID]:
    """Seed both strand shapes; return (shape_i_job_id, shape_ii_job_id).

    Shape (i): pending row, actor_config PRESENT, queue no worker serves,
    schedule_to_close NULL (invisible to the deadline sweep).
    Shape (ii): pending singleton row whose actor_config is then deleted,
    schedule_to_close NULL (so enqueue's retry_after is None forever).
    """
    await conn.executemany(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
        [
            (_SHAPE_I_ACTOR, "default"),
            (_SHAPE_II_ACTOR, "default"),
        ],
    )
    shape_i_job = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at) "
        "VALUES ($1, $2, $3, '{}'::jsonb, 3, 'transient', 'pending', clock_timestamp())",
        shape_i_job,
        _SHAPE_I_ACTOR,
        _NO_WORKER_QUEUE,
    )
    shape_ii_job = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at, metadata) "
        "VALUES ($1, $2, 'default', '{}'::jsonb, 3, 'transient', 'pending', clock_timestamp(), "
        "'{\"singleton\": true}'::jsonb)",
        shape_ii_job,
        _SHAPE_II_ACTOR,
    )
    await conn.execute(f'DELETE FROM "{schema}".actor_config WHERE actor = $1', _SHAPE_II_ACTOR)
    return shape_i_job, shape_ii_job


async def test_stranded_detector_sees_both_strand_shapes(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The detector must count BOTH strand shapes. Today it counts only the
    deleted-actor_config shape — a pending row WITH actor_config on an
    unserved queue is invisible to the gauge."""
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await _seed_strand_shapes(conn, schema)

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        gauge = await _run_stranded_detector_once(pg_dsn, pool, schema, monkeypatch)

        assert _SHAPE_I_ACTOR in gauge and _SHAPE_II_ACTOR in gauge, (
            "Contract: the stranded-jobs detector must see every permanently-undispatchable "
            "pending row (or an equivalent signal must alarm on it). Current behavior violates "
            f"it: the detector published {gauge!r} — shape (i) (a pending row WITH actor_config "
            f"on queue {_NO_WORKER_QUEUE!r}, which no worker serves: dispatch's lateral "
            "annihilates it via `j2.queue = sq.queue_name`, "
            "src/taskq/backend/_dispatch_sql.py:229, and schedule_to_close NULL keeps the "
            "deadline sweep off it) is invisible; only shape (ii) (deleted actor_config) is "
            "counted."
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_stranded_singleton_blocker_refuses_every_enqueue_forever(pg_dsn: str) -> None:
    """Amplifier evidence: a singleton blocker stranded in the no-exit pending
    cell makes every later enqueue for that actor raise SingletonCollisionError
    with retry_after=None — permanently, because nothing can exit the blocker.

    Pins today's permanent refusal: two enqueues, spaced apart, both refused
    with no advisory retry hint (schedule_to_close is NULL, so
    src/taskq/backend/_enqueue.py:550-574 computes retry_after=None).
    """
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        _, shape_ii_job = await _seed_strand_shapes(conn, schema)

        backend = _pool_free_backend(schema)
        args = make_enqueue_args(actor=_SHAPE_II_ACTOR, metadata={"singleton": True})

        refusals: list[tuple[UUID | None, object]] = []
        for _attempt in range(2):
            with pytest.raises(SingletonCollisionError) as excinfo:
                await backend.enqueue_with_conn(conn, args)
            refusals.append((excinfo.value.blocking_job_id, excinfo.value.retry_after))
            # Space the attempts: the blocker cannot exit between them — no
            # worker serves its actor (actor_config deleted), the deadline
            # sweep ignores it (schedule_to_close NULL), and it is not running.
            await asyncio.sleep(0.05)

        assert all(
            blocking == shape_ii_job and retry_after is None for blocking, retry_after in refusals
        ), (
            "Contract: a singleton refusal must not be permanent — the blocking row must be "
            "exitable, or the refusal must carry a retry hint. Current behavior violates it: "
            f"both enqueue attempts (spaced 50ms apart) were refused with blocking_job_id="
            f"{shape_ii_job!r} (the stranded blocker) and retry_after=None — the blocker sits "
            f"in the no-exit pending cell (actor_config deleted, schedule_to_close NULL), so "
            "every enqueue for this actor is refused forever with no advisory hint."
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── The assignment-routed strand shape ───────────────────────────────


_ASSIGNMENT_ACTOR = "assignment_routed_actor"
_SERVED_LABEL_QUEUE = "served-label-queue"
_UNSERVED_ASSIGNMENT_QUEUE = "unserved-assignment-queue"


async def _seed_assignment_routed_strand(conn: asyncpg.Connection, schema: str) -> UUID:
    """Seed a re-pended row whose routing queue no worker serves.

    A re-pended row (``status='pending'`` with ``started_at`` set) is
    routed by its actor's stored assignment, not by the queue label it
    carries — the label survives only as an audit trail of where the row
    was originally placed. So the row below is claimable by a consumer of
    ``_UNSERVED_ASSIGNMENT_QUEUE`` and by no one else, while its label
    still names a queue the fleet does serve.
    """
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
        _ASSIGNMENT_ACTOR,
        _UNSERVED_ASSIGNMENT_QUEUE,
    )
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, max_attempts, retry_kind, status, attempt, "
        " scheduled_at, started_at, assignment_routed) "
        "VALUES ($1, $2, $3, '{}'::jsonb, 3, 'transient', 'pending', 1, "
        " clock_timestamp(), clock_timestamp(), true)",
        job_id,
        _ASSIGNMENT_ACTOR,
        _SERVED_LABEL_QUEUE,
    )
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',
        new_uuid(),
        "worker-host",
        4242,
        [_SERVED_LABEL_QUEUE],
    )
    return job_id


async def _claims(
    conn: asyncpg.Connection, schema: str, queues: list[str], *, rounds: int = 3
) -> set[UUID]:
    """Run real dispatch rounds for a consumer of *queues*; return claimed ids."""
    claimed: set[UUID] = set()
    for _round in range(rounds):
        rows = await dispatch_batch_sql(
            conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=schema),
            queues=queues,
            limit_n=10,
            worker_id=new_uuid(),
            lock_lease=timedelta(seconds=30),
        )
        claimed.update(row["id"] for row in rows)
    return claimed


async def test_repended_row_routed_to_an_unserved_queue_is_undispatchable(
    pg_dsn: str,
) -> None:
    """A re-pended row whose actor's stored assignment names a queue no
    worker serves can never be claimed, however many consumers run.

    This is the strand shape a queue move leaves behind when the target
    queue's consumers were never stood up, and it is the one an operator
    is least equipped to reason about: the row is pending, due, and
    carries a queue label the fleet visibly serves, so every surface that
    reads the label says the work is on a healthy queue. Dispatch does not
    read the label for such a row — the assignment routes it — so the only
    consumer that could claim it is the one nobody is running.

    The control half of the pin matters as much as the failure half: a
    consumer of the assignment queue claims the row immediately, which is
    what makes "unclaimable" a statement about the fleet's subscriptions
    rather than about the row being malformed.
    """
    schema = f"tarq_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        job_id = await _seed_assignment_routed_strand(conn, schema)

        by_label_consumer = await _claims(conn, schema, [_SERVED_LABEL_QUEUE])
        assert job_id not in by_label_consumer, (
            "setup expectation: the row's routing queue is the actor's stored "
            "assignment, so a consumer of the label queue must not claim it"
        )

        row = await conn.fetchrow(
            f'SELECT status, queue FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert row is not None
        assert row["status"] == "pending", (
            "the row must still be pending and due after the label consumer's "
            f"rounds; it is {row['status']!r}"
        )
        assert row["queue"] == _SERVED_LABEL_QUEUE, (
            "the row must still carry the label naming a served queue, which is "
            "what makes the strand invisible to every label-keyed surface"
        )

        by_assignment_consumer = await _claims(conn, schema, [_UNSERVED_ASSIGNMENT_QUEUE])
        assert job_id in by_assignment_consumer, (
            "control: a consumer of the actor's stored assignment queue must "
            "claim the row, proving it is dispatchable work stranded by the "
            "fleet's subscriptions rather than an unclaimable row"
        )
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_stranded_detector_sees_the_assignment_routed_strand(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stranded-jobs gauge must count a re-pended row whose actor's
    stored assignment names a queue no worker serves.

    The detector is the fleet's only surface that can decide "no worker
    anywhere serves this" — a single booting worker cannot, which is why
    it warns instead of refusing. It answers that question by testing the
    row's queue label against the ``workers`` table. For a re-pended row
    the label is not the routing queue, so the detector asks its question
    about the wrong queue: it reports healthy while the row is
    permanently undispatchable.

    An operator hits this after moving an actor onto a queue whose
    consumers were never started. Nothing fails: the jobs are pending and
    due, the queue on their label is served, the gauge is zero, and the
    retries simply never run.
    """
    schema = f"tarq_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await _seed_assignment_routed_strand(conn, schema)

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        gauge = await _run_stranded_detector_once(pg_dsn, pool, schema, monkeypatch)

        assert gauge.get(_ASSIGNMENT_ACTOR, 0) >= 1, (
            "the stranded-jobs gauge must count the re-pended row routed to "
            f"{_UNSERVED_ASSIGNMENT_QUEUE!r}, which no worker serves; the gauge "
            f"published {gauge!r}. The detector tests the row's queue LABEL "
            f"({_SERVED_LABEL_QUEUE!r}, which a live worker does serve) against "
            "the workers table, but a re-pended row is routed by its actor's "
            "stored assignment — so the one surface that can see a fleet-wide "
            "strand reports healthy while the work can never be claimed"
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


# ── Per-shape attribution: one row, one category, the right queue name ──────

_GHOST_ACTOR = "ghost_repend_actor"
_GHOST_LABEL_QUEUE = "ghost-served-label-queue"
_STRAY_ACTOR = "post_move_stray_actor"
_STRAY_SERVED_ASSIGNMENT = "stray-served-assignment-queue"
_STRAY_UNSERVED_LABEL = "stray-unserved-label-queue"


async def test_repend_with_no_actor_config_reports_only_the_config_shape(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A re-pended row whose actor has no config row is ONE strand, reported
    in the config category alone.

    Without a config row there is no assignment to route by, so "is the
    routing queue served" has no meaning for the row — and the row's own
    label is never its routing queue once the marker is set. A detector
    that nonetheless evaluates the unserved-queue arm against the missing
    assignment (NULL) reports the row twice: once as the config strand it
    is, and once as an unserved-queue strand naming a label that a live
    worker provably serves — an operator chasing that event hunts a queue
    problem that does not exist while the real cause (seed the actor's
    config row) goes unnamed.
    """
    schema = f"tshp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind, status, attempt, "
            " scheduled_at, started_at, assignment_routed) "
            "VALUES ($1, $2, $3, '{}'::jsonb, 3, 'transient', 'pending', 1, "
            " clock_timestamp(), clock_timestamp(), true)",
            new_uuid(),
            _GHOST_ACTOR,
            _GHOST_LABEL_QUEUE,
        )
        # A live worker serving the row's LABEL queue: the strongest form of
        # the contrast — even with the label served, the NULL-assignment arm
        # must not report an unserved queue.
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',
            new_uuid(),
            "worker-host",
            4242,
            [_GHOST_LABEL_QUEUE],
        )

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        gauge, events = await _run_detector(pg_dsn, pool, schema, monkeypatch)

        assert gauge.get(_GHOST_ACTOR) == 1, (
            "the config-missing re-pend is one stranded row and must count once "
            f"in the gauge; published {gauge!r}"
        )
        config_events = [e for e in events if e.get("event") == "stranded-jobs-no-actor-config"]
        assert [e.get("actor") for e in config_events] == [_GHOST_ACTOR], (
            "the row must be reported as the missing-config strand exactly once; "
            f"events={config_events!r}"
        )
        assert config_events[0].get("pending_count") == 1
        unserved_events = [
            e
            for e in events
            if e.get("event") == "stranded-jobs-unserved-queue" and e.get("actor") == _GHOST_ACTOR
        ]
        assert unserved_events == [], (
            "a row stranded for a missing config row must not ALSO be reported as "
            "an unserved-queue strand — with no config row there is no assignment "
            f"to test, and its label ({_GHOST_LABEL_QUEUE!r}, served by a live "
            f"worker) is not its routing queue; events={unserved_events!r}"
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_unserved_event_names_the_queue_dispatch_would_route_by(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The queue an unserved-queue warning names must be the queue dispatch
    would actually route the row by — the queue the detector TESTED.

    Two rows, one per routing class: a producer-placed stray left on a
    retired source queue after its actor moved (label-routed: the label is
    unserved while the actor's current assignment IS served), and a
    re-pended row whose assignment names a queue nothing serves while its
    label names a served one. Naming anything but the tested queue sends
    the operator to subscribe consumers to a queue that is already served
    — or to retire one that is not the problem.
    """
    schema = f"tshp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await conn.executemany(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2)',
            [(_STRAY_ACTOR, _STRAY_SERVED_ASSIGNMENT)],
        )
        # The post-move producer stray: label-routed (never re-pended), so
        # dispatch serves it by its own label — the retired queue.
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at) "
            "VALUES ($1, $2, $3, '{}'::jsonb, 3, 'transient', 'pending', clock_timestamp())",
            new_uuid(),
            _STRAY_ACTOR,
            _STRAY_UNSERVED_LABEL,
        )
        await _seed_assignment_routed_strand(conn, schema)
        # One worker serving the queues that ARE served in this scenario:
        # the stray actor's current assignment and the re-pended row's label.
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',
            new_uuid(),
            "worker-host",
            4242,
            [_STRAY_SERVED_ASSIGNMENT, _SERVED_LABEL_QUEUE],
        )

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        _gauge, events = await _run_detector(pg_dsn, pool, schema, monkeypatch)

        unserved = {
            e.get("actor"): e for e in events if e.get("event") == "stranded-jobs-unserved-queue"
        }
        stray_event = unserved.get(_STRAY_ACTOR)
        assert stray_event is not None and stray_event.get("queues") == [_STRAY_UNSERVED_LABEL], (
            f"the label-routed stray is dispatched by its own label, so the event "
            f"must name the unserved label {_STRAY_UNSERVED_LABEL!r} — naming the "
            f"actor's assignment {_STRAY_SERVED_ASSIGNMENT!r} (which a live worker "
            f"serves) reports the healthy queue as the problem; event={stray_event!r}"
        )
        repend_event = unserved.get(_ASSIGNMENT_ACTOR)
        assert repend_event is not None and repend_event.get("queues") == [
            _UNSERVED_ASSIGNMENT_QUEUE
        ], (
            f"the re-pended row is dispatched by its actor's assignment, so the "
            f"event must name the unserved assignment {_UNSERVED_ASSIGNMENT_QUEUE!r} "
            f"— naming the label {_SERVED_LABEL_QUEUE!r} (served) reports the "
            f"healthy queue as the problem; event={repend_event!r}"
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
