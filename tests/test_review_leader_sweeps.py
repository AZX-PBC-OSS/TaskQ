# ruff: noqa: S608  # Why: every interpolated identifier is a generated test schema name; all values are $-bound.
"""Focused review attacks: leader election, the sweep loop, the advisory locks.

Five shapes the existing suites had not pinned:

* ``_release_session_lock`` vs PG's reentrant session-lock semantics: a
  session whose hold COUNT is above one (an earlier unconfirmed unlock
  left one count, the same pooled session re-acquired) must be drained
  to zero, not popped once - a single pop strands the fleet's lock on a
  live pooled session exactly like the leak ``test_rt_locks_prune_unlock_leak``
  pinned for the failed-unlock shape.
* The previous holder RACING a peer for its own lapsed row: the own-row
  arm is unconditional on liveness, so the two arms can both match the
  same lapsed row in the same instant. The upsert serialises on the row
  and exactly one candidate may win.
* ``_drain_bounded``'s demotion clause for the EVERY-TICK sweeps: a
  leader demoted mid-drain stops between committed batches (the prune
  family's gate has this pin; the shared runner's did not).
* Spec isolation across ticks: one sweep's UNEXPECTED failure skips the
  sibling sweeps for that tick (the guard boundary is the spec loop),
  but must cost neither the loop's life nor the siblings' next tick.
* ``_sleep_until_next_attempt``'s deferral-floor boundary: a retry rung
  that is no earlier than the next cron fire IS the scheduled fire (the
  fresh-sequence arm), one strictly earlier is a retry (the ladder arm).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import ModulePgSchema, _create_worker
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import (
    _drain_bounded,
    _release_session_lock,
    _sleep_until_next_attempt,
)
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import build_leader_lease_sql

# Lock names are per-test strings; the review probe needs no schema table.
_STACK_PROBE_LOCK = "review:stacked_hold_probe"


# ── Unit doubles ──────────────────────────────────────────────────────────


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakeConn:
    async def fetchval(self, sql: str, *args: object) -> object:
        return None

    async def execute(self, sql: str, *args: object) -> str:
        return "DELETE 0"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    def is_closed(self) -> bool:
        return False


class _FakePool:
    """asyncpg.Pool stand-in yielding one fixed fake conn."""

    def __init__(self, conn: _FakeConn | None = None) -> None:
        self._conn = conn or _FakeConn()

    @contextlib.asynccontextmanager
    async def acquire(self, *, timeout: float | None = None):  # type: ignore[no-untyped-def]  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword signature.
        yield self._conn


def _worker_settings(**overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x"}
    for key, value in overrides.items():
        data[f"TASKQ_{key}"] = value
    return WorkerSettings.load_from_dict(data, validate=False)


def _make_deps(leading: bool, *, sweep_drain_batches: int = 5) -> WorkerDeps:
    """WorkerDeps with fake pools; ``leading`` pre-sets the leader flag.

    With no term set, ``leading()`` follows the bare flag, so the tests
    can flip leadership by clearing the event (``stop_leading``).
    """
    settings = _worker_settings(
        HEARTBEAT_INTERVAL="0.5",
        HEARTBEAT_COMMAND_TIMEOUT="0.1",
        LOCK_LEASE="3.0",
        WATCHDOG_LOOP_LAG_BUDGET="1.2",
        WATCHDOG_LOOP_LAG_WARN_BUDGET="0.5",
        MAX_HEARTBEAT_FAILURES="3",
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
        SWEEP_DRAIN_BATCHES=str(sweep_drain_batches),
        SWEEP_INTERVAL="0.01",
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: FakePool drop-in for asyncpg.Pool in unit tests.
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]
    )
    if leading:
        deps.is_leader.set()
    return deps


def _make_ctx(deps: WorkerDeps, backend: object) -> SweepContext:
    return SweepContext(
        deps=deps,
        backend=backend,  # type: ignore[arg-type]  # Why: the surfaces under test are the drain gate and the spec loop, both driven through the scripted callables.
        clock=object(),  # type: ignore[arg-type]
        worker_id=new_uuid(),
    )


# ── Advisory locks: the reentrant hold-count strand (real PG) ────────────


@pytest.mark.integration
async def test_release_session_lock_drains_a_stacked_hold_to_zero(pg_dsn: str) -> None:
    """One release call must leave a REENTRANTLY-stacked hold fully gone.

    The strand shape: an attempt's release ends unconfirmed (both the
    plain unlock and the verdict retry fail transiently on a session that
    survives), leaving the pooled session holding one count. The next
    attempt on that same session re-acquires - ``pg_try_advisory_lock``
    is reentrant per session, so it returns true and the count reaches
    two - and the attempt's release pops ONE count, reports success, and
    returns. Every other pod then reads the lock held until that session
    is recycled, with no further release ever reaching zero.

    Contract: the release drains the session's hold to zero - after one
    ``_release_session_lock`` call, another session can acquire.
    """
    holder = await asyncpg.connect(pg_dsn)
    bystander = await asyncpg.connect(pg_dsn)
    try:
        # Two stacked holds on one session: exactly the count state the
        # strand shape leaves behind (one unconfirmed + one re-acquire).
        first = await holder.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", _STACK_PROBE_LOCK
        )
        second = await holder.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", _STACK_PROBE_LOCK
        )
        assert first is True and second is True, (
            "setup: PG advisory locks are reentrant per session"
        )

        await _release_session_lock(holder, _STACK_PROBE_LOCK, kind="review_probe")

        acquired = await bystander.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", _STACK_PROBE_LOCK
        )
        assert acquired is True, (
            "one _release_session_lock call must drain a stacked hold to zero: a "
            "single pg_advisory_unlock pops one reentrant count and leaves the "
            "session holding the lock, so every other pod reads the prune/"
            "archive-expiry lock held until the pooled session is recycled"
        )
        if acquired:
            await bystander.execute(
                "SELECT pg_advisory_unlock(hashtextextended($1, 0))", _STACK_PROBE_LOCK
            )
    finally:
        for conn in (holder, bystander):
            with contextlib.suppress(Exception):
                while await conn.fetchval(
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))", _STACK_PROBE_LOCK
                ):
                    pass
            await conn.close()


# ── Election: the previous holder racing a peer for its own lapsed row ───


@pytest.mark.integration
async def test_holder_racing_a_peer_for_its_own_lapsed_row_elects_exactly_one_winner(
    module_pg_schema: ModulePgSchema,
) -> None:
    """The own-row arm and the takeover arm can match the same lapsed row
    in the same instant - the previous holder re-electing while a peer's
    takeover predicate reads the row's pre-refresh state. The upsert
    serialises on the singleton row, so exactly ONE candidate wins per
    instant, and the loser's conflict update re-evaluates against the
    winner's fresh row and matches nothing.
    """
    schema = module_pg_schema.schema_name
    holder_id = new_uuid()
    peer_id = new_uuid()
    conn_h = await asyncpg.connect(module_pg_schema.pg_dsn)
    conn_p = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        await _create_worker(conn_h, schema, holder_id)
        await _create_worker(conn_p, schema, peer_id)
        elect_sql, _, _ = build_leader_lease_sql(schema)

        for round_no in range(25):
            # A lapsed row naming the previous holder: both arms' predicates
            # match this state simultaneously.
            await conn_h.execute(f'DELETE FROM "{schema}".maintenance_leader')
            await conn_h.execute(
                f'INSERT INTO "{schema}".maintenance_leader '
                f"(singleton, worker_id, elected_at, last_seen_at, expires_at) "
                f"VALUES (true, $1, clock_timestamp() - interval '1 hour', "
                f"clock_timestamp() - interval '1 hour', "
                f"clock_timestamp() - interval '1 hour')",
                holder_id,
            )

            # A barrier start, then both candidates race: whichever
            # statement commits first wins; the other re-evaluates its
            # conflict predicate against the winner's fresh row.
            start = asyncio.Event()
            start.set()

            async def _race(
                conn: asyncpg.Connection,
                wid: UUID,
                *,
                gate: asyncio.Event = start,
                sql: str = elect_sql,
            ) -> object:
                await gate.wait()
                return await conn.fetchval(sql, wid, 3600.0, 3600.0)

            holder_won, peer_won = await asyncio.gather(
                _race(conn_h, holder_id), _race(conn_p, peer_id)
            )
            winners = [w for w in (holder_won, peer_won) if w is not None]
            assert len(winners) == 1, (
                f"round {round_no}: the own-row arm and the takeover arm both matched "
                f"the same lapsed row (holder={holder_won is not None}, "
                f"peer={peer_won is not None}) - two leaders, every sweep and cron "
                "tick running twice"
            )
            row_worker = await conn_h.fetchval(
                f'SELECT worker_id FROM "{schema}".maintenance_leader WHERE singleton = true'
            )
            assert row_worker in (holder_id, peer_id)
    finally:
        await conn_h.close()
        await conn_p.close()


# ── Sweep loop: the every-tick drain's demotion boundary ─────────────────


async def test_sweep_drain_stops_at_batch_boundary_on_demotion() -> None:
    """A leader demoted mid-drain stops between committed batches.

    Same boundary the prune family's gate pins: each batch is committed,
    so stopping is a pause the successor resumes from, and nothing further
    starts under a pod that no longer leads. The stop is a PAUSE, not a
    failure: the drain reports clean so a demotion-cut tick does not feed
    the unexpected-error streak.
    """
    drain_calls = 0

    async def _always_rows() -> int:
        nonlocal drain_calls
        drain_calls += 1
        return 7  # never zero: the drain only stops on its gates

    deps = _make_deps(leading=True, sweep_drain_batches=5)
    ctx = _make_ctx(deps, backend=object())
    shutdown = asyncio.Event()

    clean = await _drain_bounded(
        ctx,
        shutdown,
        sweep_name="expired_locks",
        call=_always_rows,
        warn_event="sweep-expired-locks-failed",
        warn_kind="sweep_expired_locks_failed",
    )
    # No demotion yet: 5 batches - 1 = 4 further calls after the initial.
    assert drain_calls == 4
    assert clean is True

    # Now the demotion mid-drain: leadership drops after the FIRST drain
    # call, and the drain must stop there instead of running the remaining
    # three batches.
    drain_calls = 0

    async def _demote_after_first() -> int:
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            deps.stop_leading()
        return 7

    clean = await _drain_bounded(
        ctx,
        shutdown,
        sweep_name="expired_locks",
        call=_demote_after_first,
        warn_event="sweep-expired-locks-failed",
        warn_kind="sweep_expired_locks_failed",
    )
    assert drain_calls == 1, (
        f"the demoted leader ran {drain_calls} drain call(s) after the flag dropped; "
        "leader-only work must stop at the batch boundary"
    )
    assert clean is True, "a demotion-cut drain is a pause, not a failed iteration"


# ── Sweep loop: spec isolation across ticks ──────────────────────────────


async def test_one_sweeps_unexpected_failure_costs_the_siblings_one_tick_not_the_loop() -> None:
    """One sweep's unexpected error skips the sibling specs for THAT tick
    (the guard boundary is the spec loop) but costs neither the loop's
    life nor the siblings' next tick: the very next iteration runs the
    sibling again, and the backstop's streak counts exactly one.
    """
    import structlog.testing

    from taskq.worker import _leader_sweeps as sweeps_mod

    deadline_calls = 0
    reclaim_calls = 0

    class _OneShotBuggyBackend:
        async def reclaim_expired_locks(self, cg: timedelta, ug: timedelta) -> int:
            nonlocal reclaim_calls
            reclaim_calls += 1
            if reclaim_calls == 1:
                raise ValueError("one sweep's bug")
            return 0

        async def deadline_sweep(self) -> int:
            nonlocal deadline_calls
            deadline_calls += 1
            return 0

    deps = _make_deps(leading=True, sweep_drain_batches=2)
    ctx = _make_ctx(deps, backend=_OneShotBuggyBackend())
    shutdown = asyncio.Event()

    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))
    try:
        with structlog.testing.capture_logs() as captured:
            await wait_for_condition(
                lambda: deadline_calls >= 2,
                description="the sibling sweep must run again on the next tick",
                timeout=10.0,
            )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    unexpected = [e for e in captured if e.get("event") == "loop-unexpected-error"]
    assert [e.get("consecutive") for e in unexpected] == [1], (
        "exactly one unexpected error, tolerated - the loop must survive it"
    )
    assert deadline_calls >= 2, (
        "the sibling sweep's first tick was cut by the buggy spec; the next tick "
        f"must run it again (deadline_calls={deadline_calls})"
    )


# ── Cadence: the deferral-floor boundary of the retry ladder ─────────────


async def test_retry_rung_no_earlier_than_the_fire_is_the_scheduled_fire() -> None:
    """``_sleep_until_next_attempt``'s boundary: a retry rung that is not
    strictly earlier than the next cron fire ends AT the fire, so the
    scheduled fire governs the wake (a fresh backoff sequence), while a
    strictly earlier rung is a retry (the ladder continues from its
    current rung).
    """
    shutdown = asyncio.Event()
    fire_soon = datetime.now(UTC) + timedelta(seconds=0.1)
    # Retry rung LONGER than the time left: the sleep ends at the fire,
    # not at the rung - the fresh-sequence arm.
    woke_for_retry = await _sleep_until_next_attempt(shutdown, fire_soon, 5.0)
    assert woke_for_retry is False, (
        "a rung no earlier than the next fire must end AT the fire and be "
        "reported as the scheduled fire, so yesterday's capped rung does not "
        "carry into today's attempt"
    )
    assert not shutdown.is_set()

    # Retry rung strictly earlier: the ladder arm, reported as a retry.
    fire_later = datetime.now(UTC) + timedelta(seconds=0.5)
    woke_for_retry = await _sleep_until_next_attempt(shutdown, fire_later, 0.1)
    assert woke_for_retry is True, (
        "a rung strictly earlier than the fire must be reported as a retry so "
        "the failure sequence continues from its current rung"
    )
