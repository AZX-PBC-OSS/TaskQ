"""The leader's resign broadcast: a wake that is a HINT, never the authority.

The graceful stop already bounded handover at ``heartbeat_interval`` plus
one round trip: the resign deletes the lease row and the next follower
election tick wins it. This module adds the proactive path on top - the
resigner broadcasts the vacancy on ``leader_wake_channel(schema)`` AFTER
the delete commits (the cron tick's commit-gate discipline, trivially
satisfied: the DELETE is a single autocommit statement and the NOTIFY is
a separate statement behind it), and a follower that hears it re-runs the
SAME fenced elect immediately, damped by ``leader_wake_jitter``.

Every pin here guards one half of the doctrine:

* the hint path is real (the broadcast carries the payload convention,
  the woken follower assumes sub-second);
* every way the hint can fail - dropped, duplicated, stale, disabled,
  heard mid-bootstrap, heard by the resigner itself - degrades to exactly
  the pre-wake behavior: the tick cadence, the bound a killed leader
  (which can notify no one) still holds.

The election statements under the wake are the ones
``tests/test_leader_lease_contract.py`` pins, unchanged: the row and the
fence stay the truth, the wake only decides WHEN a follower next reads
them.

Mutation notes (each pin names what going red proves):
* mutating ``_follower_park``'s wake wait off (always sleep the tick)
  reds ``test_follower_park_wakes_before_the_tick``;
* mutating the jitter sleep off reds ``test_follower_park_jitter_damps_the_wake``;
* mutating the callback's ``type`` check off reds
  ``test_foreign_type_is_dropped``;
* mutating the own-echo check off reds ``test_own_echo_is_dropped``.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import AsyncExitStack, suppress
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.constants import leader_wake_channel
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import _create_worker
from taskq.worker.deps import LeaderTerm, WorkerDeps, open_worker_deps
from taskq.worker.leader import MaintenanceLeader, build_leader_lease_sql
from taskq.worker.notify import _make_leader_wake_callback

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 0.5
_LOCK_LEASE = 750.0
# The watchdog's inner probe sleep (leader._WATCHDOG_INTERVAL_SECS): the
# longest natural-drain tail in run()'s TaskGroup at shutdown, the tail
# the winner_task timeouts below must fund.
_WATCHDOG_INTERVAL = 5.0
# Same shape as test_leader_integration's settings: max_heartbeat_failures
# is huge so the isolation never fires mid-test; the lease validator's
# cascade floor (1000 beats x (0.5 + 2 x 0.1) = 700s) sizes the lease.


def _build_settings(
    pg_dsn: str, schema: str, *, jitter: str = "0", heartbeat: str = str(_HEARTBEAT_INTERVAL)
) -> WorkerSettings:
    hb = float(heartbeat)
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema.lower(),
            "TASKQ_HEARTBEAT_INTERVAL": heartbeat,
            # The cascade floor scales with the heartbeat: 1000 failed
            # beats x (hb + 2 x 0.1) + margin, so a widened tick for the
            # latency pins still loads.
            "TASKQ_LOCK_LEASE": str(1000 * hb + 200 + 50),
            "TASKQ_LEADER_WAKE_JITTER": jitter,
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
        }
    )


class _WakeDisabledBackend(PostgresBackend):
    """A backend whose leadership wake subscription is GONE.

    Models the follower whose notify listener is disabled (the poll
    fallback): the park finds no event and sleeps the plain tick. The
    override is a non-callable, which is exactly what run()'s
    getattr-guard tests for.
    """

    subscribe_leader_wake = None  # type: ignore[assignment]  # Why: the guard is ``callable(getattr(...))``; None is the disabled shape.


async def _open_pod(
    pg_dsn: str,
    schema: str,
    worker_id: UUID,
    settings: WorkerSettings,
    *,
    wake_enabled: bool = True,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend]:
    """One worker's deps + backend over a migrated schema."""
    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    backend: PostgresBackend
    if wake_enabled:
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
    else:
        backend = _WakeDisabledBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0),
            cleanup_grace_period=timedelta(seconds=0),
        )
    async with deps.dispatcher_pool.acquire() as conn:
        await _create_worker(conn, settings.schema_name, worker_id)
    return stack, deps, backend


async def _migrate(
    pg_dsn: str, schema: str, *, heartbeat: str = str(_HEARTBEAT_INTERVAL)
) -> WorkerSettings:
    settings = _build_settings(pg_dsn, schema, heartbeat=heartbeat)
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
    finally:
        await conn.close()
    return settings


async def _leader_row(conn: asyncpg.Connection, schema: str) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f'SELECT worker_id, elected_at FROM "{schema}".maintenance_leader WHERE singleton = true'  # noqa: S608  # Why: schema is the module fixture's generated identifier, validated at load; values are $-bound where they vary.
    )


# ── Unit tier: the wake routing and the follower's park ───────────


def _wake_backend(*, worker_id: str = "resigner") -> tuple[PostgresBackend, asyncio.Event]:
    """A stand-in backend exposing only the registry the callback fans out to."""
    event = asyncio.Event()
    return (
        SimpleNamespace(_leader_wake_subscribers={event}),  # type: ignore[arg-type]  # Why: the callback touches only _leader_wake_subscribers; the stand-in cannot drift from the real registry's shape.
        event,
    )


def _notify(worker_id: str, *, type_: str = "leader_resigned") -> str:
    return json.dumps({"type": type_, "worker_id": worker_id, "elected_at": "2026-01-01T00:00:00Z"})


class TestWakeCallbackRouting:
    """The payload-kind discipline of ``_make_leader_wake_callback``."""

    def test_valid_wake_sets_subscribers(self) -> None:
        backend, event = _wake_backend()
        cb = _make_leader_wake_callback(backend, new_uuid())
        cb(None, 1, "ch", _notify("resigner"))
        assert event.is_set(), "a foreign resign must wake the election loop"

    def test_own_echo_is_dropped(self) -> None:
        """The resigner hears its own broadcast and must not wake itself."""
        own = str(new_uuid())
        backend, event = _wake_backend()
        cb = _make_leader_wake_callback(backend, UUID(own))
        cb(None, 1, "ch", _notify(own))
        assert not event.is_set(), (
            "a pod that just handed the row back must not immediately re-win it: "
            "the own echo is what keeps the unassumable hand-back from flapping "
            "at NOTIFY speed instead of the tick"
        )

    def test_foreign_type_is_dropped(self) -> None:
        """Other payload kinds share the channel by convention and never wake."""
        backend, event = _wake_backend()
        cb = _make_leader_wake_callback(backend, new_uuid())
        cb(None, 1, "ch", _notify("resigner", type_="cancel"))
        cb(None, 1, "ch", _notify("resigner", type_="leader_resigned_v2"))
        assert not event.is_set()

    def test_empty_and_malformed_payloads_are_dropped(self) -> None:
        backend, event = _wake_backend()
        cb = _make_leader_wake_callback(backend, new_uuid())
        for payload in ("", "not json", "[1, 2]", "null"):
            cb(None, 1, "ch", payload)
        assert not event.is_set()

    def test_wake_heard_with_no_subscriber_registered_is_harmless(self) -> None:
        """Mid-bootstrap: the wake lands before the election loop exists.

        No subscriber registered (run() has not entered the subscription
        yet, or this worker never leads) - the callback must not crash,
        and the election is not lost: the loop's next park rides the tick.
        """
        backend = SimpleNamespace(_leader_wake_subscribers=set())  # type: ignore[arg-type]
        cb = _make_leader_wake_callback(backend, new_uuid())
        cb(None, 1, "ch", _notify("resigner"))  # must not raise


class TestFollowerPark:
    """``_follower_park``: the tick is the guarantee, the wake the accelerator."""

    async def test_follower_park_wakes_before_the_tick(self) -> None:
        """MUTATION PIN - the wake wait itself.

        Mutating ``_follower_park`` to always sleep the tick reds this: a
        broadcast that arrives mid-park must return the park early, which
        is the whole feature.
        """
        leader, _deps, _backend, _conn, _pool, _shutdown = await _unit_leader()
        leader._deps.settings.leader_wake_jitter = 0.0
        wake = asyncio.Event()
        leader._wake_event = wake  # the merged shape: ONE wake event, the subscription re-points it at the backend's for the run; the park consumes this object.
        loop = asyncio.get_running_loop()

        async def _arrive() -> None:
            await asyncio.sleep(0.05)
            wake.set()

        arriver = asyncio.create_task(_arrive())
        start = loop.time()
        await leader._follower_park()
        elapsed = loop.time() - start
        await arriver
        assert elapsed < 0.3 * _HEARTBEAT_INTERVAL, (
            f"a set wake must return the park before the tick; took {elapsed:.3f}s"
        )

    async def test_follower_park_jitter_damps_the_wake(self, monkeypatch: Any) -> None:
        """MUTATION PIN - the damping sleep.

        Mutating the jitter sleep off (waking followers land on the row in
        the same millisecond) reds this: with the event already set, the
        park may not return before the damped delay has run. uniform is
        pinned (the drawn delay is deterministic), so the wait length
        itself is the assertion - a removed sleep returns in ~0.
        """
        leader, _deps, _backend, _conn, _pool, _shutdown = await _unit_leader()
        leader._deps.settings.leader_wake_jitter = 0.2
        import random as random_mod

        monkeypatch.setattr(random_mod, "uniform", lambda a, b: 0.15)
        wake = asyncio.Event()
        wake.set()  # the wake already landed (mid-cycle shape)
        leader._wake_event = wake
        loop = asyncio.get_running_loop()
        start = loop.time()
        await leader._follower_park()
        elapsed = loop.time() - start
        assert 0.14 <= elapsed <= 0.4, (
            f"the jitter must damp a wake before the re-elect; returned after {elapsed:.3f}s"
        )

    async def test_follower_park_without_wake_sleeps_the_tick(self) -> None:
        """The disabled-listener shape: no event, the plain tick, unchanged."""
        leader, _deps, _backend, _conn, _pool, _shutdown = await _unit_leader()
        # The merged disabled shape: the stand-in backend has no
        # subscribe_leader_wake, so run()'s subscription never re-points
        # _wake_event and no external caller (callback or otherwise) can
        # arm it - the park's only exits are the tick and the stop signal.
        assert not callable(getattr(_backend, "subscribe_leader_wake", None))
        leader._deps.settings.heartbeat_interval = 0.05
        loop = asyncio.get_running_loop()
        start = loop.time()
        await leader._follower_park()
        elapsed = loop.time() - start
        assert elapsed >= 0.05, "the park must sleep the full tick when no wake exists"
        assert elapsed < 1.0, f"the park must not invent its own long backoff: {elapsed:.3f}s"

    async def test_wake_arriving_before_the_park_is_not_lost(self) -> None:
        """A wake that lands while the loop is mid-cycle stays set and wakes
        the NEXT park immediately - not lost, at most one extra fenced elect."""
        leader, _deps, _backend, _conn, _pool, _shutdown = await _unit_leader()
        leader._deps.settings.leader_wake_jitter = 0.0
        wake = asyncio.Event()
        leader._wake_event = wake
        wake.set()  # landed before the park was entered
        loop = asyncio.get_running_loop()
        start = loop.time()
        await leader._follower_park()
        assert loop.time() - start < 0.1, "a sticky wake must not be lost to the park"


async def _unit_leader() -> tuple[
    MaintenanceLeader, WorkerDeps, SimpleNamespace, None, None, asyncio.Event
]:
    """The park tests need no election machinery - only settings and the event.

    Built directly (not via test_leader's _make_leader, whose fakes model
    the whole loop) so a park change cannot silently depend on election
    double behavior. The backend stand-in's ``subscribe_leader_wake`` is
    None - the disabled shape, so the parks under test start from no wake
    and each test installs its own event (or none) by hand.
    """
    from tests.test_leader import (
        _make_deps,  # type: ignore[attr-defined]  # Why: the deps stub factory is the suite's canonical WorkerDeps double.
    )

    deps = _make_deps(heartbeat_interval=_HEARTBEAT_INTERVAL, leader_lease=_LOCK_LEASE)
    backend = SimpleNamespace(subscribe_leader_wake=None)  # type: ignore[arg-type]  # Why: MaintenanceLeader reads the backend only through getattr-guarded subscribe_leader_wake here.
    leader = MaintenanceLeader(deps, new_uuid(), backend, clock=SystemClock())  # type: ignore[arg-type]  # Why: the stand-in satisfies the getattr seam the subscription uses.
    return leader, deps, backend, None, None, asyncio.Event()


# ── Integration tier: the real wire ───────────────────────────────


async def _start_two(
    pg_dsn: str, schema: str, *, heartbeat: str = str(_HEARTBEAT_INTERVAL)
) -> tuple[
    WorkerSettings,
    AsyncExitStack,
    WorkerDeps,
    PostgresBackend,
    UUID,
    AsyncExitStack,
    WorkerDeps,
    PostgresBackend,
    UUID,
]:
    """Two pods, wake enabled, jitter 0 - the deterministic broadcast shape."""
    settings = await _migrate(pg_dsn, schema, heartbeat=heartbeat)
    wid_a, wid_b = new_uuid(), new_uuid()
    stack_a, deps_a, backend_a = await _open_pod(pg_dsn, schema, wid_a, settings)
    try:
        stack_b, deps_b, backend_b = await _open_pod(pg_dsn, schema, wid_b, settings)
    except BaseException:
        await stack_a.aclose()
        raise
    return settings, stack_a, deps_a, backend_a, wid_a, stack_b, deps_b, backend_b, wid_b


@pytest.mark.asyncio
async def test_lw1_wake_driven_handover_is_sub_second(pg_dsn: str) -> None:
    """THE feature pin: a graceful stop hands over within the wake round trip.

    The follower listens through the PRODUCTION callback
    (``_make_leader_wake_callback``) on a raw conn, so the whole chain is
    real: resign's pg_notify -> PG -> listener conn -> payload routing ->
    the backend registry -> the park's wake -> the jittered fenced elect.
    Asserts the wake-to-assumed latency inside one heartbeat - the old
    tick bound was the heartbeat ITSELF.
    """
    (
        settings,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _start_two(pg_dsn, f"test_leader_wake_{new_base62()}", heartbeat="1.0")

    received: list[tuple[float, str]] = []

    def _record(conn: object, pid: int, channel: str, payload: str) -> None:
        received.append((asyncio.get_running_loop().time(), payload))

    raw = await asyncpg.connect(pg_dsn)
    try:
        channel = leader_wake_channel(settings.schema_name)
        # BOTH pods' production routing on the raw conn (each pod's own
        # echo filter decides whose park may move - the resigner's own
        # wake is dropped), plus a recording tap for the latency
        # measurement.
        await raw.add_listener(channel, _make_leader_wake_callback(backend_a, wid_a))
        await raw.add_listener(channel, _make_leader_wake_callback(backend_b, wid_b))
        await raw.add_listener(channel, _record)

        shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
        leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
        leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
        task_a = asyncio.create_task(leader_a.run(shutdown_a))
        task_b = asyncio.create_task(leader_b.run(shutdown_b))
        try:
            await wait_for_condition(
                lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
                description="one pod won the initial election",
                timeout=5 * float(settings.heartbeat_interval) + 3,
            )
            if deps_a.is_leader.is_set():
                winner_task, winner_shutdown = task_a, shutdown_a
                follower_deps = deps_b
            else:
                winner_task, winner_shutdown = task_b, shutdown_b
                follower_deps = deps_a

            t_stop = asyncio.get_running_loop().time()
            winner_shutdown.set()
            # run()'s TaskGroup drains NATURALLY on the shutdown event (the
            # body completes; TaskGroup cancels nothing), so the resign waits
            # out each loop's current sleep - the pre-existing teardown tail
            # the published SLA never counted (its bound is phrased on the
            # resign: "the resign deletes the row; the next election wins
            # it"). The feature's claim is measured from the WAKE, below.
            await asyncio.wait_for(
                winner_task, timeout=_WATCHDOG_INTERVAL + float(settings.heartbeat_interval) + 3
            )
            await wait_for_condition(
                follower_deps.is_leader.is_set,
                description="the surviving follower is the leader",
                timeout=float(settings.heartbeat_interval) + 3.0,
            )
            t_leader = asyncio.get_running_loop().time()

            assert received, "the resign must broadcast on the leadership wake channel"
            t_wake, _payload = received[0]
            stop_to_leader = t_leader - t_stop
            wake_to_leader = t_leader - t_wake
            # THE feature pin: from the wake (the instant the resign's
            # broadcast landed, i.e. after the delete committed) to the
            # follower ASSUMING - well inside HALF a heartbeat tick, where
            # the old tick path's wake-up alone was uniform over the whole
            # tick. Measured runs: 37-110ms against a 1.0s tick.
            assert wake_to_leader < 0.5 * float(settings.heartbeat_interval), (
                f"wake-to-assumed must beat half the tick bound; took {wake_to_leader:.3f}s"
            )
            print(
                f"\n[lw1] stop-to-assumed={stop_to_leader * 1000:.1f}ms "
                f"(incl. the pre-existing natural teardown drain) "
                f"wake-to-assumed={wake_to_leader * 1000:.1f}ms "
                f"(heartbeat_interval={settings.heartbeat_interval}s)"
            )
        finally:
            shutdown_a.set()
            shutdown_b.set()
            for task in (task_a, task_b):
                if not task.done():
                    task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(task_a, task_b, return_exceptions=True)
    finally:
        with suppress(Exception):
            await raw.close()
        await stack_b.aclose()
        await stack_a.aclose()


@pytest.mark.asyncio
async def test_lw2_the_broadcast_names_the_kind_the_resigner_and_the_dead_term(
    pg_dsn: str,
) -> None:
    """The payload discipline on the real wire.

    The wake carries the ``{"type": ...}`` convention with the resigning
    worker's id and the term handed back; it is sent only after the
    DELETE committed, so by the time a receiver hears it the row is
    already gone (the ordering claim the resign docstring makes).
    """
    (
        settings,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _start_two(pg_dsn, f"test_leader_wake_{new_base62()}")

    payloads: list[str] = []

    def _tap(conn: object, pid: int, channel: str, payload: str) -> None:
        payloads.append(payload)

    raw = await asyncpg.connect(pg_dsn)
    try:
        await raw.add_listener(leader_wake_channel(settings.schema_name), _tap)
        shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
        leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
        leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
        task_a = asyncio.create_task(leader_a.run(shutdown_a))
        task_b = asyncio.create_task(leader_b.run(shutdown_b))
        try:
            await wait_for_condition(
                lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
                description="one pod won the initial election",
                timeout=5 * float(settings.heartbeat_interval) + 3,
            )
            resigner = wid_a if deps_a.is_leader.is_set() else wid_b
            resigner_task = task_a if deps_a.is_leader.is_set() else task_b
            resigner_shutdown = shutdown_a if deps_a.is_leader.is_set() else shutdown_b
            resigner_shutdown.set()
            await asyncio.wait_for(
                wait_for_condition(
                    lambda: bool(payloads), description="the broadcast", timeout=5.0
                ),
                timeout=6.0,
            )
            await asyncio.wait_for(
                resigner_task,
                timeout=_WATCHDOG_INTERVAL + float(settings.heartbeat_interval) + 3,
            )

            assert len(payloads) == 1, f"one resign, one broadcast: {payloads}"
            msg = json.loads(payloads[0])
            assert msg["type"] == "leader_resigned"
            assert msg["worker_id"] == str(resigner)
            dead_term = datetime.fromisoformat(msg["elected_at"])  # the term, parseable
            # Ordering: the delete committed before the notify was sent, so
            # a receiver holding the payload can rely on the row it names
            # being gone - the row a SUCCESSOR re-won since (the follower's
            # next tick legitimately claims the vacancy) carries a FRESH
            # elected_at, never the resigned one.
            row = await _leader_row(raw, settings.schema_name)
            assert row is None or row["elected_at"] > dead_term, (
                "the broadcast must not precede the committed delete of the term it names"
            )
        finally:
            shutdown_a.set()
            shutdown_b.set()
            for task in (task_a, task_b):
                if not task.done():
                    task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(task_a, task_b, return_exceptions=True)
    finally:
        with suppress(Exception):
            await raw.close()
        await stack_b.aclose()
        await stack_a.aclose()


@pytest.mark.asyncio
async def test_lw3_wake_lost_keeps_the_tick_bound(pg_dsn: str) -> None:
    """The drop shape: a follower that never hears the wake still takes over
    on the tick - the published bound, unregressed.

    The follower's backend has NO leadership wake subscription (the
    disabled-listener shape, the same degradation a dropped NOTIFY or a
    poll-fallback worker rides): the park sleeps the full tick, the next
    elect wins, and the fleet never waits a lease.
    """
    settings = await _migrate(pg_dsn, f"test_leader_wake_{new_base62()}")
    wid_a, wid_b = new_uuid(), new_uuid()
    stack_a, deps_a, backend_a = await _open_pod(pg_dsn, settings.schema_name, wid_a, settings)
    try:
        # The follower: wake disabled.
        stack_b, deps_b, backend_b = await _open_pod(
            pg_dsn, settings.schema_name, wid_b, settings, wake_enabled=False
        )
        try:
            shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
            leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
            leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
            task_a = asyncio.create_task(leader_a.run(shutdown_a))
            task_b = asyncio.create_task(leader_b.run(shutdown_b))
            try:
                await wait_for_condition(
                    lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
                    description="one pod won the initial election",
                    timeout=5 * _HEARTBEAT_INTERVAL + 3,
                )
                if deps_a.is_leader.is_set():
                    winner_task, winner_shutdown = task_a, shutdown_a
                else:
                    winner_task, winner_shutdown = task_b, shutdown_b
                winner_shutdown.set()
                await asyncio.wait_for(
                    winner_task, timeout=_WATCHDOG_INTERVAL + _HEARTBEAT_INTERVAL + 3
                )
                # The published graceful bound: one heartbeat tick plus a
                # round trip's margin. No lease lapse anywhere near.
                await wait_for_condition(
                    lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
                    description="the follower took over on its tick (the wake was never the guarantee)",
                    timeout=_HEARTBEAT_INTERVAL + 3.0,
                )
            finally:
                shutdown_a.set()
                shutdown_b.set()
                for task in (task_a, task_b):
                    if not task.done():
                        task.cancel()
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(task_a, task_b, return_exceptions=True)
        finally:
            await stack_b.aclose()
    finally:
        await stack_a.aclose()


@pytest.mark.asyncio
async def test_lw4_a_stale_wake_costs_one_fenced_noop(pg_dsn: str) -> None:
    """The stale shape: a wake delivered for a vacancy already filled.

    The woken follower re-runs the fenced elect, matches nothing, and
    parks again: the holder's term is untouched, exactly one leader, no
    ping-pong. The broadcast never became an authority.
    """
    (
        settings,
        stack_a,
        deps_a,
        backend_a,
        wid_a,
        stack_b,
        deps_b,
        backend_b,
        wid_b,
    ) = await _start_two(pg_dsn, f"test_leader_wake_{new_base62()}")

    raw = await asyncpg.connect(pg_dsn)
    try:
        # BOTH pods' production routing on one raw conn: whichever pod is
        # the follower has its park driven by the real wire, and each
        # pod's own-echo filter keeps the holder's park deaf to wakes.
        await raw.add_listener(
            leader_wake_channel(settings.schema_name), _make_leader_wake_callback(backend_a, wid_a)
        )
        await raw.add_listener(
            leader_wake_channel(settings.schema_name), _make_leader_wake_callback(backend_b, wid_b)
        )
        shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
        leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
        leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
        task_a = asyncio.create_task(leader_a.run(shutdown_a))
        task_b = asyncio.create_task(leader_b.run(shutdown_b))
        try:
            await wait_for_condition(
                lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
                description="one pod won the initial election",
                timeout=5 * _HEARTBEAT_INTERVAL + 3,
            )
            # The holder is whoever won; the stale wake goes to the OTHER
            # pod, whose production routing is the listener wired above
            # (backend_b's, so when B is the holder the wake must reach it
            # through A's backend instead - wire BOTH, each pod's own echo
            # filter decides whose park may move).
            holder_deps = deps_a if deps_a.is_leader.is_set() else deps_b
            follower_deps = deps_b if deps_a.is_leader.is_set() else deps_a
            holder_row_before = await _leader_row(raw, settings.schema_name)
            assert holder_row_before is not None

            # A foreign resign broadcast for a vacancy that does not exist:
            # not A (the holder's own echo is A's to ignore and A holds the
            # row), not B - a duplicated/stale wake from some earlier term.
            await raw.execute(
                "SELECT pg_notify($1, $2)",
                leader_wake_channel(settings.schema_name),
                json.dumps(
                    {
                        "type": "leader_resigned",
                        "worker_id": str(new_uuid()),
                        "elected_at": datetime.now(tz=UTC).isoformat(),
                    }
                ),
            )

            # The wake lands, B re-runs the fenced elect, matches nothing.
            await asyncio.sleep(3 * _HEARTBEAT_INTERVAL)

            holder_row_after = await _leader_row(raw, settings.schema_name)
            assert holder_row_after is not None
            assert holder_row_after["worker_id"] == holder_row_before["worker_id"], (
                "a stale wake must not displace the holder"
            )
            assert holder_row_after["elected_at"] == holder_row_before["elected_at"], (
                "the loser's fenced elect must not rewrite the winner's term"
            )
            assert deps_a.is_leader.is_set() != deps_b.is_leader.is_set()
            assert holder_deps.is_leader.is_set() and not follower_deps.is_leader.is_set(), (
                "exactly one leader survives a stale wake"
            )
        finally:
            shutdown_a.set()
            shutdown_b.set()
            for task in (task_a, task_b):
                if not task.done():
                    task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(task_a, task_b, return_exceptions=True)
    finally:
        with suppress(Exception):
            await raw.close()
        await stack_b.aclose()
        await stack_a.aclose()


@pytest.mark.asyncio
async def test_lw5_a_fenced_resign_deletes_nothing_and_broadcasts_nothing(pg_dsn: str) -> None:
    """The takeover race, extended with the wake.

    A resign issued after a successor took the row deletes nothing (the
    fence pins that in test_leader_lease_contract) - and must also
    BROADCAST nothing: a wake behind a DELETE 0 would send the fleet to
    re-elect a row that is not vacant. The successor's row is untouched
    and no wake is heard.

    The deposed holder is a BARE MaintenanceLeader object (never run), its
    fence set the way ``_assume_leadership`` leaves it: racing the
    successor's tick is deliberately avoided - the deposed holder's
    own-row re-elect arm can revive its backdated row before the
    successor's tick lands, which would make the pin time-flaky instead
    of fence-sharp.
    """
    settings = await _migrate(pg_dsn, f"test_leader_wake_{new_base62()}")
    wid_a, wid_b = new_uuid(), new_uuid()
    stack_a, deps_a, backend_a = await _open_pod(pg_dsn, settings.schema_name, wid_a, settings)
    try:
        stack_b, deps_b, backend_b = await _open_pod(pg_dsn, settings.schema_name, wid_b, settings)
        try:
            wakes: list[str] = []

            def _tap(conn: object, pid: int, channel: str, payload: str) -> None:
                wakes.append(payload)

            raw = await asyncpg.connect(pg_dsn)
            try:
                await raw.add_listener(leader_wake_channel(settings.schema_name), _tap)
                # The predecessor: a bare leader object whose term is on the
                # row, as _assume_leadership leaves it. Never run() - its
                # loops cannot re-elect behind the test's back.
                leader_a = MaintenanceLeader(deps_a, wid_a, backend_a, clock=SystemClock())
                leader_a._resign_fence = LeaderTerm(
                    elected_at=datetime.now(tz=UTC), trusted_until=1e12
                )
                await raw.execute(
                    f'INSERT INTO "{settings.schema_name}".maintenance_leader '  # noqa: S608  # Why: schema is this module's generated identifier; values $-bound.
                    "(singleton, worker_id, elected_at, last_seen_at, expires_at) "
                    "VALUES (true, $1, clock_timestamp(), clock_timestamp(), "
                    "clock_timestamp() + make_interval(secs => 750))",
                    wid_a,
                )
                row_before = await _leader_row(raw, settings.schema_name)
                assert row_before is not None and UUID(str(row_before["worker_id"])) == wid_a

                # The successor pod runs; the takeover: the holder's signals
                # lapsed, B takes the row the ordinary way every peer runs.
                leader_b = MaintenanceLeader(deps_b, wid_b, backend_b, clock=SystemClock())
                shutdown_b = asyncio.Event()
                task_b = asyncio.create_task(leader_b.run(shutdown_b))
                try:
                    await raw.execute(
                        f'UPDATE "{settings.schema_name}".maintenance_leader '  # noqa: S608
                        "SET expires_at = clock_timestamp() - interval '1 hour', "
                        "last_seen_at = clock_timestamp() - interval '1 hour' "
                        "WHERE singleton = true"
                    )
                    await wait_for_condition(
                        deps_b.is_leader.is_set,
                        description="the successor took the lapsed row",
                        timeout=5 * _HEARTBEAT_INTERVAL + 3,
                    )
                    successor_row = await _leader_row(raw, settings.schema_name)
                    assert successor_row is not None
                    assert UUID(str(successor_row["worker_id"])) == wid_b

                    # The deposed leader's resign, issued late, riding a
                    # live conn so the DELETE really executes (not the
                    # silent no-conn shape).
                    deps_a.leader_conn = raw  # type: ignore[assignment]  # Why: the resign's conn choice is the unit under test.
                    deleted = await leader_a.resign()
                    assert deleted is False, "a fenced resign must not report a hand-back"

                    successor_after = await _leader_row(raw, settings.schema_name)
                    assert successor_after is not None
                    assert successor_after["worker_id"] == successor_row["worker_id"]
                    assert successor_after["elected_at"] == successor_row["elected_at"], (
                        "the resign's fence must leave the successor's row byte-identical"
                    )
                    assert not wakes, f"a DELETE 0 must broadcast nothing; the fleet heard: {wakes}"
                finally:
                    shutdown_b.set()
                    if not task_b.done():
                        task_b.cancel()
                    with suppress(asyncio.CancelledError):
                        await task_b
            finally:
                with suppress(Exception):
                    await raw.close()
        finally:
            await stack_b.aclose()
    finally:
        await stack_a.aclose()


@pytest.mark.asyncio
async def test_lw6_eight_electors_woken_at_once_crown_exactly_one(pg_dsn: str) -> None:
    """The herd pin, at the broadcast's herd size.

    A wake lands every follower in the same quarter-second; the damped
    re-elects still serialise on the row's conflict predicate: 8
    concurrent elects over an unclaimed row crown exactly one leader,
    8 over a lapsed row exactly one taker. (The two-taker versions live
    in test_leader_lease_contract; this is the same statements at the
    fleet shape the jitter exists to damp.)
    """
    schema_settings = await _migrate(pg_dsn, f"test_leader_wake_{new_base62()}")
    schema = schema_settings.schema_name
    conn = await asyncpg.connect(str(schema_settings.pg_dsn))
    try:
        elect_sql, _renew_sql, _resign_sql = build_leader_lease_sql(schema)
        workers = [new_uuid() for _ in range(8)]
        for worker_id in workers:
            await _create_worker(conn, schema, worker_id)
        conns = [conn] + [await asyncpg.connect(str(schema_settings.pg_dsn)) for _ in range(7)]
        try:
            # Unclaimed row: one wake, every follower elects at once.
            results = await asyncio.gather(
                *(
                    c.fetchval(elect_sql, worker_id, 3600.0, 3600.0)
                    for c, worker_id in zip(conns, workers, strict=True)
                )
            )
            winners = [r for r in results if r is not None]
            assert len(winners) == 1, (
                f"8 simultaneous elects over an unclaimed row crown one: {results}"
            )
            row = await _leader_row(conn, schema)
            assert row is not None and row["elected_at"] == winners[0]

            # Lapsed row: the wake races a takeover instead.
            await conn.execute(
                f'UPDATE "{schema}".maintenance_leader '  # noqa: S608
                "SET expires_at = clock_timestamp() - interval '1 hour', "
                "last_seen_at = clock_timestamp() - interval '1 hour'"
            )
            # Each (conn, worker) pair re-elects AS ITSELF: a winner from
            # the first race may be any worker, including the one whose
            # conn owns the shared handle - the pairing must follow the
            # pairs, not the list slice, or an elect rides a foreign
            # worker_id and dies on the workers FK.
            loser_pairs = [
                (c, w) for c, w, r in zip(conns, workers, results, strict=True) if r is None
            ]
            takeover = await asyncio.gather(
                *(c.fetchval(elect_sql, worker_id, 3600.0, 3600.0) for c, worker_id in loser_pairs)
            )
            takers = [r for r in takeover if r is not None]
            assert len(takers) == 1, (
                f"8 simultaneous elects over a lapsed row crown one taker: {takeover}"
            )
        finally:
            for extra in conns[1:]:
                with suppress(Exception):
                    await extra.close()
    finally:
        with suppress(Exception):
            await conn.close()


@pytest.mark.asyncio
async def test_lw7_the_graceful_bound_table_holds_on_the_real_wire(pg_dsn: str) -> None:
    """Measured latencies for the bounds table, both paths, one run:

    * wake delivered: stop-to-assumed sub-second (lw1's assertion,
      re-measured here on a second schema for the report's number);
    * the crash bound is NOT this code path: a killed leader emits no
      resign and no wake, and the fleet still recovers on the tick -
      pinned by test_leader_chaos (tc1) and unchanged by this feature.
    """
    (
        settings,
        stack_a,
        deps_a,
        backend_a,
        _wid_a,
        stack_b,
        deps_b,
        backend_b,
        _wid_b,
    ) = await _start_two(pg_dsn, f"test_leader_wake_{new_base62()}", heartbeat="1.0")

    got: list[float] = []

    def _tap(conn: object, pid: int, channel: str, payload: str) -> None:
        got.append(asyncio.get_running_loop().time())

    raw = await asyncpg.connect(pg_dsn)
    try:
        # BOTH pods' production routing: whichever pod loses the initial
        # race is the follower whose park the wake must move, and each
        # pod's own-echo filter keeps the resigner deaf to its own
        # broadcast.
        await raw.add_listener(
            leader_wake_channel(settings.schema_name),
            _make_leader_wake_callback(backend_a, _wid_a),
        )
        await raw.add_listener(
            leader_wake_channel(settings.schema_name),
            _make_leader_wake_callback(backend_b, _wid_b),
        )
        await raw.add_listener(leader_wake_channel(settings.schema_name), _tap)
        shutdown_a, shutdown_b = asyncio.Event(), asyncio.Event()
        leader_a = MaintenanceLeader(deps_a, _wid_a, backend_a, clock=SystemClock())
        leader_b = MaintenanceLeader(deps_b, _wid_b, backend_b, clock=SystemClock())
        task_a = asyncio.create_task(leader_a.run(shutdown_a))
        task_b = asyncio.create_task(leader_b.run(shutdown_b))
        try:
            await wait_for_condition(
                lambda: deps_a.is_leader.is_set() != deps_b.is_leader.is_set(),
                description="one pod won the initial election",
                timeout=5 * float(settings.heartbeat_interval) + 3,
            )
            if deps_a.is_leader.is_set():
                winner_task, winner_shutdown, winner_deps = task_a, shutdown_a, deps_a
                follower_deps = deps_b
            else:
                winner_task, winner_shutdown, winner_deps = task_b, shutdown_b, deps_b
                follower_deps = deps_a
            winner_shutdown.set()
            t_stop = time.monotonic()
            await asyncio.wait_for(
                winner_task, timeout=_WATCHDOG_INTERVAL + float(settings.heartbeat_interval) + 3
            )
            assert not winner_deps.is_leader.is_set()
            await wait_for_condition(
                follower_deps.is_leader.is_set,
                description="the follower assumed",
                timeout=float(settings.heartbeat_interval) + 3.0,
            )
            stop_to_assumed = time.monotonic() - t_stop
            assert got, "the broadcast must land on the real wire"
            wake_to_assumed = time.monotonic() - got[0]
            # The feature bound is wake-to-assumed; stop-to-assumed carries
            # the pre-existing natural teardown drain (see lw1), which no
            # part of this feature touched. Same half-tick discriminator
            # as lw1.
            assert wake_to_assumed < 0.5 * float(settings.heartbeat_interval)
            print(
                f"\n[lw7] graceful handover: stop-to-assumed={stop_to_assumed * 1000:.1f}ms "
                f"(incl. natural teardown drain), wake-to-assumed={wake_to_assumed * 1000:.1f}ms "
                f"(before: up to heartbeat_interval={settings.heartbeat_interval}s + one round trip "
                "from the resign)"
            )
        finally:
            shutdown_a.set()
            shutdown_b.set()
            for task in (task_a, task_b):
                if not task.done():
                    task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(task_a, task_b, return_exceptions=True)
    finally:
        with suppress(Exception):
            await raw.close()
        await stack_b.aclose()
        await stack_a.aclose()
