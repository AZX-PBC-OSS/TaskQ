"""ATTACK tests for hunt/terminal-provenance (PR 375).

Every test here is an executed attack against the pushed branch state,
asserted through the public surfaces a client/consumer/operator observes:
the job read model (``list_jobs``), the attempt history (``get_attempts``),
the event log (``get_events``), the reclaim feed (``poll_reclaim_events``)
and cancellation visibility (``poll_cancel_flags``). A failure is a RED.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import EventRow, JobFilter, JobId
from taskq.backend._sweeps import sweep_expired_locks
from taskq.constants import (
    CANCEL_ORIGIN_COOPERATIVE,
    CANCEL_ORIGIN_FORCED,
    CANCEL_ORIGIN_UNREQUESTED,
)
from taskq.settings import WorkerSettings
from taskq.testing.pg import setup_running_job
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import isolate_self

if TYPE_CHECKING:
    from taskq.testing.fixtures import JobsApp

pytestmark = pytest.mark.integration

# The documented origin truth table: phase 2 interrupt -> forced; a request
# on the row -> cooperative; neither -> unrequested (the runtime's own
# cancel, no operator request may be forged). The origin is what the
# operator reads off the job and its history.
FORCED = CANCEL_ORIGIN_FORCED
COOPERATIVE = CANCEL_ORIGIN_COOPERATIVE
UNREQUESTED = CANCEL_ORIGIN_UNREQUESTED


async def _running_row(
    conn: object,
    schema: str,
    *,
    cancel_phase: int = 0,
    cancel_requested_at: datetime | None = None,
    attempt: int = 1,
    max_attempts: int = 3,
) -> tuple[UUID, UUID]:
    worker_id, job_id = await setup_running_job(
        conn,  # type: ignore[arg-type]
        schema,
        worker_id=new_uuid(),
        cancel_phase=cancel_phase,
        cancel_requested_at=cancel_requested_at,
        attempt=attempt,
        max_attempts=max_attempts,
        retry_kind="transient",
    )
    return worker_id, job_id


def _iso_deps(app: "JobsApp") -> WorkerDeps:

    deps = app.deps
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": deps.settings.resolved_pg_dsn_direct,
            "TASKQ_SCHEMA_NAME": deps.settings.schema_name,
        }
    )
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=deps.dispatcher_pool,  # type: ignore[arg-type]
        heartbeat_pool=deps.heartbeat_pool,  # type: ignore[arg-type]
        worker_pool=deps.worker_pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


async def _observable_state(
    app: "JobsApp", job_id: JobId
) -> tuple[str, str | None, datetime | None]:
    """The public read model of one job: (status, origin marker, cancel
    request visibility)."""
    rows = await app.backend.list_jobs(JobFilter())
    mine = [row for row in rows if row.id == job_id]
    assert len(mine) == 1, f"attack broken: job {job_id} not in the public list"
    row = mine[0]
    return row.status, row.error_class, row.cancel_requested_at


async def _isolate_feed_events(app: "JobsApp", job_id: JobId) -> list[EventRow]:
    """The reclaim feed's deliveries for one job (public feed contract)."""
    feed = await app.backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
    return [event for event in feed if event.job_id == job_id]


# Bounds every injected wait below so a wedged interleaving fails, never hangs.
_WAIT = 15.0


class _ParkedIsolateConn:
    """Real connection wrapper that parks isolate_self at an injected
    suspension point inside its own transaction, so a race test can
    interleave the opposing writer exactly inside the race window instead
    of hoping sleep head starts sample the winning order (a scheduling
    lottery: green on a fast runner, red on a loaded one).

    isolate_self's per-row sequence on this one connection is a plain
    ``fetch`` (the running-rows SELECT, no locks taken) then ``execute``
    calls (the guarded UPDATE arbiter, the attempt INSERT, the batched
    event INSERT). The park point picks which racing side the iteration
    is forced to let win:

    - ``park_after="select"``: parked between the SELECT and the guarded
      UPDATE -- the exact window the opposing writer wins from (isolate
      has read the row as running but holds no row locks).
    - ``park_after="arbiter"``: parked after the guarded UPDATE itself,
      before the transaction's commit -- isolate holds the jobs-row lock,
      so the opposing writer's own UPDATE is forced to arbitrate against
      isolate's committed outcome.
    """

    def __init__(
        self,
        conn: Any,
        *,
        park_after: str,
        parked: asyncio.Event,
        release: asyncio.Event,
    ) -> None:
        self._conn = conn
        self._park_after = park_after
        self._parked = parked
        self._release = release
        self._fetches = 0
        self._executes = 0

    def transaction(self, **kwargs: object) -> Any:
        return self._conn.transaction(**kwargs)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        rows = await self._conn.fetch(sql, *args)
        self._fetches += 1
        if self._park_after == "select" and self._fetches == 1:
            self._parked.set()
            await asyncio.wait_for(self._release.wait(), timeout=_WAIT)
        return rows

    async def execute(self, sql: str, *args: object) -> str:
        tag = await self._conn.execute(sql, *args)
        self._executes += 1
        if self._park_after == "arbiter" and self._executes == 1:
            self._parked.set()
            await asyncio.wait_for(self._release.wait(), timeout=_WAIT)
        return tag

    async def close(self) -> None:
        await self._conn.close()

    def terminate(self) -> None:
        self._conn.terminate()


def _parked_isolate_connect(
    dsn: str, iso_conn: Any, park_after: str, parked: asyncio.Event, release: asyncio.Event
) -> Any:
    """The ``asyncpg.connect`` stand-in isolate_self's fresh connection is
    routed through: the already-open real connection behind the parking
    wrapper, with the iteration's park point bound (no loop-variable
    capture)."""

    async def fake_connect(dsn_arg: str, **_kwargs: object) -> _ParkedIsolateConn:
        assert dsn_arg == dsn
        return _ParkedIsolateConn(iso_conn, park_after=park_after, parked=parked, release=release)

    return fake_connect


async def test_cancel_origin_truth_table_all_phases(clean_jobs_app: "JobsApp") -> None:
    """Cancel in every phase x request evidence: the origin the operator
    reads off the job, its attempt history and its event log is the origin
    the row's own evidence proves. This is the PR's core claim."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    now = datetime.now(UTC)
    matrix = [
        # (cancel_phase, cancel_requested_at, expected origin)
        (0, None, UNREQUESTED),
        (1, now, COOPERATIVE),
        (2, now, FORCED),
    ]
    for phase, requested, expected in matrix:
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await _running_row(
                conn,
                schema,
                cancel_phase=phase,
                cancel_requested_at=requested,
            )
            applied = await clean_jobs_app.backend.mark_cancelled(
                job_id, worker_id, attempt=1, claim_epoch=1
            )
            assert applied, f"fixture broken at phase {phase}: cancel did not apply"

        status, origin, _ = await _observable_state(clean_jobs_app, job_id)
        assert status == "cancelled"
        assert origin == expected, (
            f"RED: phase={phase} requested={requested is not None}: the job "
            f"reports origin {origin!r}, its own evidence proves {expected!r}"
        )
        # The attempt history and the event log tell the same story as the
        # job: three surfaces, one origin.
        attempts = await clean_jobs_app.backend.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].error_class == expected, (
            f"RED: the attempt history disagrees with the job's origin at phase {phase}"
        )
        events = await clean_jobs_app.backend.get_events(job_id)
        cancel_events = [
            event
            for event in events
            if event.kind == "state_change" and event.detail.get("to_state") == "cancelled"
        ]
        assert len(cancel_events) == 1
        assert cancel_events[0].detail["error_class"] == expected, (
            f"RED: the event log disagrees with the job's origin at phase {phase}"
        )


async def test_phase2_without_request_is_not_forged_cooperative(
    clean_jobs_app: "JobsApp",
) -> None:
    """A row escalated to phase 2 with no request stamp carries no request
    evidence; the operator must not read a cooperative cancel off it."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await _running_row(
            conn, schema, cancel_phase=2, cancel_requested_at=None
        )
        await clean_jobs_app.backend.mark_cancelled(job_id, worker_id, attempt=1, claim_epoch=1)

    _status, origin, _ = await _observable_state(clean_jobs_app, job_id)
    assert origin == FORCED, (
        f"RED: a phase-2 row with no request reports {origin!r}; the "
        "interrupt evidence outranks the missing request"
    )


async def test_terminal_write_cannot_resurrect_a_cancelled_row(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: cancel after terminal. mark_cancelled on a succeeded job
    is a clean no-op (False), the job keeps its terminal outcome."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await _running_row(conn, schema)
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'succeeded',"  # Why: fixture setup, not assertion; schema is fixture-derived and _IDENT_RE-validated.
            " finished_at = clock_timestamp(), locked_by_worker = NULL,"
            " lock_expires_at = NULL WHERE id = $1",
            job_id,
        )

    applied = await clean_jobs_app.backend.mark_cancelled(
        job_id, worker_id, attempt=1, claim_epoch=1
    )
    assert applied is False, "RED: mark_cancelled terminalised a succeeded job"

    status, origin, _ = await _observable_state(clean_jobs_app, job_id)
    assert status == "succeeded", "RED: the terminal outcome was corrupted"
    assert origin is None


async def test_mark_cancelled_attempt_mismatch_is_a_noop(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: a caller that cannot present the attempt epoch never
    terminalises the job, and the operator's cancel stays visible: the
    cancel-poll surface keeps delivering it."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await _running_row(
            conn, schema, cancel_phase=1, cancel_requested_at=datetime.now(UTC)
        )
    # attempt=None: never matches, exactly as PG's NULL bind never satisfies
    # the equality.
    applied = await clean_jobs_app.backend.mark_cancelled(job_id, worker_id, attempt=None)
    assert applied is False, "RED: the attempt fence did not hold"

    status, _origin, cancel_visible = await _observable_state(clean_jobs_app, job_id)
    assert status == "running", "RED: a fenced-out job terminalised"
    assert cancel_visible is not None, "RED: the operator's cancel request vanished"
    # Cancellation visibility: the cancel-poll surface still hands the
    # request to the worker holding the job.
    flags = await clean_jobs_app.backend.poll_cancel_flags(worker_id)
    assert any(flag.job_id == job_id for flag in flags), (
        "RED: an armed cancel stopped being delivered to the worker"
    )


async def test_isolate_self_zero_running_jobs_is_a_clean_noop(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: isolate with zero running jobs signals shutdown, reports
    no jobs anywhere, and the reclaim feed stays empty."""
    shutdown = asyncio.Event()
    await isolate_self(_iso_deps(clean_jobs_app), new_uuid(), shutdown)
    assert shutdown.is_set()

    rows = await clean_jobs_app.backend.list_jobs(JobFilter())
    assert rows == [], "RED: an empty isolate manufactured jobs"
    feed = await clean_jobs_app.backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
    assert feed == [], "RED: an empty isolate emitted reclaim feed events"


async def test_isolate_self_racing_a_terminal_write(
    clean_jobs_app: "JobsApp",
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RACE, forced both ways: isolate_self (crash reclaim) vs the worker's
    own mark_cancelled terminal write, same job, same worker.

    The arbiter on BOTH sides is the server-side guarded UPDATE: whichever
    writer's guarded UPDATE commits first owns the outcome, and the loser's
    re-check no-ops. The two legal commit orders are not sampled with sleep
    head starts -- a harness that hopes N iterations sample both orders is
    a scheduling lottery (green on a fast runner, red on a loaded CI
    runner, and a random red trains people to ignore red). Each order is
    FORCED by an injected suspension point inside isolate_self's own
    transaction (``_ParkedIsolateConn`` on isolate's fresh connection, the
    repo's gated-connection pattern), alternating per iteration, so both
    arbiters win by construction:

    - forced terminal win: isolate is parked between its running-rows
      SELECT (no row locks held) and its guarded UPDATE; the terminal
      write commits 'cancelled' inside that window; isolate's UPDATE must
      then re-check against the committed outcome and no-op (lost race).
    - forced isolate win: isolate is parked after its guarded UPDATE has
      executed, before its commit -- it holds the jobs-row lock; the
      terminal write's UPDATE is issued into that contention and can only
      arbitrate once isolate commits: it must re-check to a no-op (False).

    Observable invariants under EITHER winner:
    - the job ends in exactly one legal state (never half-written);
    - the attempt history records the epoch exactly once;
    - the reclaim feed carries an isolate event iff the job was
      isolate-reclaimed (a feed event for a job the terminal write owns is
      a phantom; a missing one orphans every feed consumer).
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    iso_deps = _iso_deps(clean_jobs_app)
    dsn = str(iso_deps.settings.pg_dsn_direct)

    # The REAL connect, captured once before any monkeypatching: the loop
    # must not re-capture the previous iteration's stand-in.
    real_connect = asyncpg.connect

    for i, forced_winner in enumerate(("terminal", "isolate") * 3):
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await _running_row(conn, schema, attempt=1, max_attempts=3)

        # The park point IS the forced winner: parked in the select window
        # the terminal write wins from, parked on the arbiter's row lock
        # isolate wins from.
        park_after = "select" if forced_winner == "terminal" else "arbiter"
        parked, release = asyncio.Event(), asyncio.Event()
        iso_conn = await real_connect(dsn)

        monkeypatch.setattr(
            asyncpg,
            "connect",
            _parked_isolate_connect(dsn, iso_conn, park_after, parked, release),
        )
        try:
            isolate_task = asyncio.create_task(isolate_self(iso_deps, worker_id, asyncio.Event()))
            await asyncio.wait_for(parked.wait(), timeout=_WAIT)

            if forced_winner == "terminal":
                # isolate parked mid-window (it read the row as running and
                # mine): the terminal write commits into the window.
                applied = await backend.mark_cancelled(job_id, worker_id, attempt=1, claim_epoch=1)
                assert applied, (
                    f"fixture broken iter {i}: the terminal write no-oped "
                    "before isolate's arbiter ran"
                )
                release.set()
                await asyncio.wait_for(isolate_task, timeout=_WAIT)
            else:
                # isolate parked holding the jobs-row lock (its guarded
                # UPDATE executed, uncommitted): the terminal write is
                # issued into that contention and cannot win.
                terminal_task = asyncio.create_task(
                    backend.mark_cancelled(job_id, worker_id, attempt=1, claim_epoch=1)
                )
                release.set()
                await asyncio.wait_for(isolate_task, timeout=_WAIT)
                applied = await asyncio.wait_for(terminal_task, timeout=_WAIT)
                assert not applied, (
                    f"RED iter {i}: the terminal write applied against a row "
                    "isolate's guarded UPDATE already owned -- the attempt "
                    "fence did not hold"
                )

            status, origin, _requested = await _observable_state(clean_jobs_app, job_id)
            attempts = await backend.get_attempts(job_id)
            feed_events = await _isolate_feed_events(clean_jobs_app, job_id)

            if forced_winner == "terminal":
                assert status == "cancelled", (
                    f"RED iter {i}: the terminal write committed inside the "
                    f"window but the job reads {status!r}"
                )
                assert origin == UNREQUESTED, (
                    f"RED iter {i}: the runtime's own unrequested cancel stamped origin {origin!r}"
                )
                assert feed_events == [], (
                    f"RED iter {i}: the terminal write owns the outcome but the "
                    "feed still advertises an isolate reclaim for the job"
                )
            else:
                # isolate owns the outcome; the budget arm applies (transient,
                # attempt 1 of 3), so the re-pend reads pending.
                assert status == "pending", (
                    f"RED iter {i}: the isolate owns the outcome but the job reads {status!r}"
                )
                assert len(feed_events) == 1, (
                    f"RED iter {i}: an isolate-owned reclaim delivered "
                    f"{len(feed_events)} feed events for one job"
                )
                assert feed_events[0].detail["cause"] == "isolate_self"
            assert len(attempts) == 1, (
                f"RED iter {i}: the attempt history shows {len(attempts)} records "
                "for one attempt (a lost or duplicated write)"
            )
        finally:
            release.set()
            await iso_conn.close()


async def test_isolate_self_racing_the_reclaim_sweep(
    clean_jobs_app: "JobsApp",
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RACE, forced both ways: isolate_self vs the leader's reclaim sweep,
    both arbiters on the same lease-expired job. Exactly one wins; the
    loser must not crash, duplicate the attempt history, or double-deliver
    the feed event.

    Both arbiters' guarded UPDATEs decide, so the two legal commit orders
    are FORCED by the injected suspension point (``_ParkedIsolateConn``),
    alternating per iteration -- not sampled by timing (a scheduling
    lottery random-fails on a loaded CI runner):

    - forced sweep win: isolate is parked between its running-rows SELECT
      (no row locks held) and its guarded UPDATE; the sweep reclaims the
      expired row inside that window. isolate's UPDATE must re-check
      against the committed outcome and no-op (lost race).
    - forced isolate win: isolate is parked after its guarded UPDATE has
      executed, before its commit -- it holds the jobs-row lock; the
      sweep's ``FOR UPDATE SKIP LOCKED`` snap must step over the
      contended row and reclaim nothing, and the row's outcome is
      isolate's.

    The winner is deterministic per iteration, so the outcome's own
    signature is asserted, not just its shape: the sweep's reclaim stamps
    'WorkerCrashed' on the attempt history and a feed event naming its
    deadline; isolate's stamps 'HeartbeatLost' and cause 'isolate_self'.
    Either way: exactly one attempt row, exactly one feed event.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    iso_deps = _iso_deps(clean_jobs_app)
    dsn = str(iso_deps.settings.pg_dsn_direct)
    expired = datetime.now(UTC) - timedelta(seconds=5)

    # The REAL connect, captured once before any monkeypatching: the loop
    # must not re-capture the previous iteration's stand-in.
    real_connect = asyncpg.connect

    for i, forced_winner in enumerate(("sweep", "isolate") * 3):
        async with deps.worker_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                retry_kind="non_retryable",
                attempt=3,
                max_attempts=3,
                lock_expires_at=expired,
            )

        park_after = "select" if forced_winner == "sweep" else "arbiter"
        parked, release = asyncio.Event(), asyncio.Event()
        iso_conn = await real_connect(dsn)

        monkeypatch.setattr(
            asyncpg,
            "connect",
            _parked_isolate_connect(dsn, iso_conn, park_after, parked, release),
        )

        async def _sweep() -> int:
            async with deps.worker_pool.acquire() as sweep_conn:
                return await sweep_expired_locks(
                    sweep_conn, timedelta(0), timedelta(0), schema=schema
                )

        try:
            isolate_task = asyncio.create_task(isolate_self(iso_deps, worker_id, asyncio.Event()))
            await asyncio.wait_for(parked.wait(), timeout=_WAIT)

            if forced_winner == "sweep":
                # isolate parked mid-window (it read the row as running and
                # mine, no locks held): the sweep reclaims it in the window.
                reclaimed = await _sweep()
                assert reclaimed == 1, (
                    f"fixture broken iter {i}: the sweep did not reclaim the expired row"
                )
                release.set()
                await asyncio.wait_for(isolate_task, timeout=_WAIT)
            else:
                # isolate parked holding the jobs-row lock (its guarded
                # UPDATE executed, uncommitted): the sweep's SKIP LOCKED
                # snap runs into that contention and must step over the row.
                sweep_task = asyncio.create_task(_sweep())
                release.set()
                await asyncio.wait_for(isolate_task, timeout=_WAIT)
                reclaimed = await asyncio.wait_for(sweep_task, timeout=_WAIT)
                assert reclaimed == 0, (
                    f"RED iter {i}: the sweep reclaimed a row isolate's "
                    "guarded UPDATE already owned -- a double reclaim "
                    "double-writes the attempt history"
                )

            status, _origin, _requested = await _observable_state(clean_jobs_app, job_id)
            assert status == "crashed", (
                f"RED iter {i}: an exhausted non_retryable job must read "
                f"crashed under either arbiter, got {status!r}"
            )
            attempts = await backend.get_attempts(job_id)
            assert len(attempts) == 1, (
                f"RED iter {i}: the attempt history shows {len(attempts)} records "
                "for one attempt (a double write)"
            )
            feed = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
            delivered = [event for event in feed if event.job_id == job_id]
            assert len(delivered) == 1, (
                f"RED iter {i}: the feed delivered {len(delivered)} reclaim events "
                "for one reclaim (a consumer double-counts the job as outstanding)"
            )
            if forced_winner == "sweep":
                assert attempts[0].error_class == "WorkerCrashed", (
                    f"RED iter {i}: the sweep owns the reclaim but the attempt "
                    f"history reads {attempts[0].error_class!r}"
                )
                assert delivered[0].detail["cause"] in {
                    "lock_expired",
                    "heartbeat_timeout",
                }, (
                    f"RED iter {i}: the sweep's feed event names cause "
                    f"{delivered[0].detail['cause']!r}, no deadline"
                )
            else:
                assert attempts[0].error_class == "HeartbeatLost", (
                    f"RED iter {i}: the isolate owns the reclaim but the attempt "
                    f"history reads {attempts[0].error_class!r}"
                )
                assert delivered[0].detail["cause"] == "isolate_self", (
                    f"RED iter {i}: an isolate-owned reclaim's feed event "
                    f"names cause {delivered[0].detail['cause']!r}"
                )
        finally:
            release.set()
            await iso_conn.close()


async def test_double_isolate_delivers_one_event_per_job(
    clean_jobs_app: "JobsApp",
) -> None:
    """API misuse: isolate twice, concurrently, same worker. The job still
    terminalises once and the feed delivers one event."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(
            conn,
            schema,
            retry_kind="non_retryable",
            attempt=3,
            max_attempts=3,
        )

    await asyncio.gather(
        isolate_self(_iso_deps(clean_jobs_app), worker_id, asyncio.Event()),  # type: ignore[arg-type]
        isolate_self(_iso_deps(clean_jobs_app), worker_id, asyncio.Event()),  # type: ignore[arg-type]
    )

    status, _origin, _requested = await _observable_state(clean_jobs_app, job_id)
    assert status == "crashed"
    attempts = await clean_jobs_app.backend.get_attempts(job_id)
    assert len(attempts) == 1, "RED: the attempt history recorded the epoch twice"
    feed_events = await _isolate_feed_events(clean_jobs_app, job_id)
    assert len(feed_events) == 1, (
        f"RED: a double isolate delivered {len(feed_events)} reclaim events for one job"
    )


async def test_sweep_and_isolate_feed_event_contents_agree(
    clean_jobs_app: "JobsApp",
) -> None:
    """Feed contract differential: every path that reclaims a job delivers
    the same channel key (the slice feed consumers tail) and a truthful
    cause naming the path; the delivered to_state matches the job's fate."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    expired = datetime.now(UTC) - timedelta(seconds=5)

    async with deps.worker_pool.acquire() as conn:
        _sweep_worker, sweep_job = await setup_running_job(
            conn,
            schema,
            retry_kind="non_retryable",
            attempt=3,
            max_attempts=3,
            lock_expires_at=expired,
        )
        iso_worker, iso_job = await setup_running_job(
            conn,
            schema,
            retry_kind="non_retryable",
            attempt=3,
            max_attempts=3,
            lock_expires_at=expired,
        )

    await isolate_self(_iso_deps(clean_jobs_app), iso_worker, asyncio.Event())  # type: ignore[arg-type]
    async with deps.worker_pool.acquire() as conn:
        n = await sweep_expired_locks(conn, timedelta(0), timedelta(0), schema=schema)
    assert n == 1

    feed = await backend.poll_reclaim_events(0, visibility_delay=timedelta(0))
    delivered = {event.job_id: event for event in feed}
    assert sweep_job in delivered and iso_job in delivered, (
        "RED: a reclaim path's events are missing from the feed"
    )
    sweep_event, iso_event = delivered[sweep_job], delivered[iso_job]
    for event in (sweep_event, iso_event):
        assert event.detail["reason"] == "lock_expired", (
            "RED: the channel key drifted; feed consumers tail this value and "
            "would never see the event"
        )
        assert event.detail["from_state"] == "running"
        assert event.detail["to_state"] == "crashed"
    assert iso_event.detail["cause"] == "isolate_self", (
        "RED: an isolate event forged the sweep's cause; a consumer cannot "
        "tell which path reclaimed the job"
    )
    assert sweep_event.detail["cause"] in {"lock_expired", "heartbeat_timeout"}, (
        f"RED: sweep cause {sweep_event.detail['cause']!r} names no deadline"
    )
    # And the causes are distinguishable: a consumer must be able to tell
    # the paths apart.
    assert iso_event.detail["cause"] != sweep_event.detail["cause"] or (
        sweep_event.detail["cause"] == "lock_expired"
    )


async def test_fenced_out_row_never_carries_a_cancel_stamp_into_a_retry(
    clean_jobs_app: "JobsApp",
) -> None:
    """The cross-branch invariant (375's isolate arms vs an armed cancel):
    a job carrying a phase-1 operator cancel that isolate reclaims must
    terminalise 'cancelled' (the operator's request honoured) and the feed
    must say so, never re-pended with the request laundered away."""
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(
            conn,
            schema,
            retry_kind="transient",
            attempt=1,
            max_attempts=3,
            cancel_phase=1,
            cancel_requested_at=datetime.now(UTC),
        )

    await isolate_self(_iso_deps(clean_jobs_app), worker_id, asyncio.Event())

    status, _origin, cancel_visible = await _observable_state(clean_jobs_app, job_id)
    assert status == "cancelled", (
        f"RED: an operator cancel in flight was re-pended ({status!r}) by "
        "the isolate's budget arm, the request was laundered"
    )
    assert cancel_visible is not None, (
        "RED: the terminalised job no longer shows the operator's request"
    )
    feed_events = await _isolate_feed_events(clean_jobs_app, job_id)
    assert len(feed_events) == 1
    assert feed_events[0].detail["to_state"] == "cancelled"
