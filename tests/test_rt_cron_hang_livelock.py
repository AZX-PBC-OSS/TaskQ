"""Red-team attacks on the cron tick's time bound vs a hung payload factory (#113).

The contract under attack: one cron tick over a due schedule whose
``payload_factory`` hangs must END — return or raise — within a bound the
deadline machinery owns, INDEPENDENT of how long the factory hangs, and
when it ends the transaction-scoped cron advisory lock must be free for the
next worker's tick. The machinery as shipped: the leader wraps the WHOLE
tick in ``asyncio.timeout(dispatcher_command_timeout)`` around its single
transaction (``worker/leader.py``, ``_cron_loop``; default 5.0 s);
``resolve_payload`` wraps ASYNC factories in ``asyncio.wait_for(...,
timeout=5.0)`` (``taskq/cron.py``; hardcoded — no setting feeds it);
``tick_cron`` itself bounds only the COUNT of due schedules — every
"deadline" mention in ``cron_loop.py`` is a docstring pointing at the
caller's machinery.

Attack 1 drives the claim's literal mechanism — an async factory parked on
an Event nothing opens — through the leader's exact nesting. The factory's
hang is unbounded and the test's bound derives only from the machinery's
own T, never from the factory: this is the calibration the existing
deadline test (``test_cron_tick_bounded.py``,
``test_deadline_tripped_tick_commits_partial_progress``) cannot express —
it calibrates its deadline to 10x a measured run of the very tick it
purports to cut short, so no deterministic slowdown of the tick can ever
trip it.

Attack 2 is the reproduction. A SYNC factory is a first-class
``resolve_payload`` shape (``cron.py`` calls ``factory()`` directly on the
event loop), and a sync factory that blocks freezes the loop INSIDE the
tick's open transaction: the 5 s ``wait_for`` never starts (the call never
becomes a coroutine) and the leader's ``asyncio.timeout`` callback cannot
run (the loop is frozen). The tick — holding the cron advisory lock taken
at its first statement — stays open for as long as the factory blocks, and
every other worker's tick returns 0 on lock contention: the fleet-wide
stall. Calibration that CAN fail: T = 5.0 s; the factory blocks 3xT; the
contract bound is T + 2.5 s. A tick that waits the factory out overruns
the bound and the test goes red with the observed duration. A second
worker's ticks run from an observer thread (own loop, own connection) at
the start of the hang and past every bound, recording the fleet-wide
effect live from the only vantage point that exists while the first
worker's loop is frozen.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.constants import schema_lock_name
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.cron_loop import tick_cron

from .test_rt_cron_harness import (
    _HOURLY,
    cron_settings,
    hour_floor,
    make_backend,
    schedule_row,
    seed_actor_config,
    seed_schedule,
)

pytestmark = pytest.mark.integration

_ACTOR = "rt_hang_actor"

#: T — the machinery's whole-tick deadline: the leader's
#: ``asyncio.timeout(dispatcher_command_timeout)`` (default 5.0 s) AND
#: ``resolve_payload``'s per-factory ``wait_for`` (hardcoded 5.0 s). The
#: attack calibrates against T alone; the factory's hang is never a
#: multiple of anything the test derives from itself.
_T_S = 5.0

#: The contract bound: the tick must end (return or raise) within T plus
#: generous slack for BEGIN, the lock probe, the due/actor reads, cancel
#: delivery and the ROLLBACK round trip — a healthy deadline trip observes
#: ~5.05 s locally, so 2.5 s of slack absorbs any runner load.
_BOUND_S = _T_S + 2.5

#: The sync factory's block: strictly longer than every bound in play (the
#: 5.0 s deadline, the 7.5 s contract bound) so the tick can only survive
#: it by being cut; bounded (not "forever") so a red test still fails in
#: bounded time.
_SYNC_BLOCK_S = 3 * _T_S

#: Attack 2's test-side safety net: strictly above every expected end (the
#: red path ends when the factory's own block ends, ~15.2 s; a bounded tick
#: ends near T) and below the file's runtime budget. It is a net for
#: unforeseen hangs, not the deadline under attack — that one is the REAL
#: ``asyncio.timeout`` inside the leader-shaped nesting.
_SAFETY_NET_S = _SYNC_BLOCK_S + 10.0

#: When the observer thread takes its second-worker tick past every bound:
#: after the contract bound, comfortably inside the factory's block.
_LATE_OFF_S = _BOUND_S + 1.0


# ── The two hung payload factories (importable dotted paths) ───────────


_HANG_STATE: dict[str, object] = {}
"""Installed by the ``_async_hang`` / ``_sync_hang`` context managers before
the tick starts; the factories read it. Module-scope mutable state is safe
here: tests run sequentially on the module-scoped event loop and always
consume the hang inside the awaited tick that set it."""


async def async_hang_factory() -> dict[str, object]:
    """Payload factory: parks on a gate nothing opens during the tick.

    Dotted path: ``tests.test_rt_cron_hang_livelock.async_hang_factory``.
    The hang is an await on an asyncio primitive the test controls —
    unbounded from the tick's perspective, cleanly cancellable — exactly
    the shape the deadline machinery is supposed to cut.
    """
    entered = _HANG_STATE["async_entered"]
    gate = _HANG_STATE["async_gate"]
    assert isinstance(entered, asyncio.Event)
    assert isinstance(gate, asyncio.Event)
    entered.set()
    await gate.wait()
    return {}


def sync_hang_factory() -> dict[str, object]:
    """Payload factory (sync): blocks the event loop inside the tick.

    Dotted path: ``tests.test_rt_cron_hang_livelock.sync_hang_factory``.
    ``resolve_payload`` calls sync factories directly on the loop
    (``taskq/cron.py``, ``result = factory()``); this one signals entry,
    then blocks the loop for ``_SYNC_BLOCK_S`` seconds. While it blocks no
    timer on the loop can fire, so no deadline machinery exists for the
    tick until the block ends.
    """
    entered = _HANG_STATE["sync_entered"]
    release = _HANG_STATE["sync_release"]
    assert isinstance(entered, threading.Event)
    assert isinstance(release, threading.Event)
    entered.set()
    release.wait(_SYNC_BLOCK_S)
    return {}


@contextlib.contextmanager
def _async_hang() -> Generator[tuple[asyncio.Event, asyncio.Event], None, None]:
    """Install the async-factory hang state; yields ``(entered, gate)``.

    ``entered`` fires when the tick reaches the factory (its transaction
    and advisory lock are open, the due set is read); ``gate`` is never
    opened inside the tick, and the ``finally`` releases anything still
    parked so no coroutine can outlive the test.
    """
    entered = asyncio.Event()
    gate = asyncio.Event()
    _HANG_STATE["async_entered"] = entered
    _HANG_STATE["async_gate"] = gate
    try:
        yield entered, gate
    finally:
        gate.set()
        _HANG_STATE.clear()


@contextlib.contextmanager
def _sync_hang() -> Generator[tuple[threading.Event, threading.Event], None, None]:
    """Install the sync-factory hang state; yields ``(entered, release)``.

    ``release`` is bounded by ``_SYNC_BLOCK_S`` inside the factory and set
    again here, so the block can never outlive the test.
    """
    entered = threading.Event()
    release = threading.Event()
    _HANG_STATE["sync_entered"] = entered
    _HANG_STATE["sync_release"] = release
    try:
        yield entered, release
    finally:
        release.set()
        _HANG_STATE.clear()


# ── The real tick path, and the observers ──────────────────────────────


async def _run_leader_shaped_tick(
    conn: asyncpg.Connection,
    settings: WorkerSettings,
    backend: PostgresBackend,
    schema: str,
    worker_id: UUID,
) -> int:
    """The leader's exact tick nesting (``worker/leader.py`` ``_cron_loop``):
    one whole-tick ``asyncio.timeout`` around one transaction around
    :func:`tick_cron`."""
    async with asyncio.timeout(settings.dispatcher_command_timeout):
        async with conn.transaction():
            return await tick_cron(conn, settings, backend, schema, worker_id)


def _second_worker_ticks_while_the_first_is_wedged(
    dsn: str,
    schema: str,
    settings: WorkerSettings,
    entered: threading.Event,
    hung_schedule_id: UUID,
    results: dict[str, Any],
) -> None:
    """A second worker's cron ticks, from a thread on its own loop and
    connection, while the first worker's loop is frozen inside the sync
    factory — the only vantage point from which the fleet-wide effect of
    the wedged tick is observable live.

    Records into *results*: ``early_fired`` / ``early_off_s`` — a tick
    taken just after the factory entered its block (0 = the cron advisory
    lock is contended: this is what every other worker in the fleet sees,
    every second, for the whole hang); ``late_fired`` / ``late_off_s`` — a
    tick taken past every bound in play (0 = the lock has now outlived the
    tick's own deadline). The hung schedule is disabled before the late
    tick so that in a world where the tick IS bounded the late tick cannot
    re-reach the factory and block the observer's loop too.
    """

    async def _tick() -> int:
        conn = await asyncpg.connect(dsn)
        try:
            async with conn.transaction():
                return await tick_cron(conn, settings, make_backend(settings), schema, new_uuid())
        finally:
            await conn.close()

    async def _disable_hung_schedule() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                f'UPDATE "{schema}".cron_schedules SET enabled = false WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
                hung_schedule_id,
            )
        finally:
            await conn.close()

    if not entered.wait(timeout=_BOUND_S + 5.0):
        results["early_fired"] = "factory never entered"
        return
    entered_at = time.monotonic()
    results["early_fired"] = asyncio.run(_tick())
    results["early_off_s"] = time.monotonic() - entered_at
    remaining = _LATE_OFF_S - (time.monotonic() - entered_at)
    if remaining > 0:
        time.sleep(remaining)
    asyncio.run(_disable_hung_schedule())
    results["late_fired"] = asyncio.run(_tick())
    results["late_off_s"] = time.monotonic() - entered_at


async def _assert_lock_free_and_cron_fires(
    conn: asyncpg.Connection,
    dsn: str,
    schema: str,
    settings: WorkerSettings,
) -> None:
    """Post-contract, reachable only when the tick ended inside the bound:
    the cron advisory lock is acquirable from a second connection and a
    tick on it fires a fresh due schedule — cron is not stalled
    fleet-wide.

    The attack's own schedules are disabled first so the probe is
    independent of what, if anything, the ended tick committed.
    """
    await conn.execute(
        f'UPDATE "{schema}".cron_schedules SET enabled = false WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; actor is $-bound.
        _ACTOR,
    )
    await seed_schedule(
        conn,
        schema,
        actor=_ACTOR,
        name="post-probe",
        cron_expr=_HOURLY,
        next_fire_at=hour_floor(datetime.now(UTC)),
    )
    probe = await asyncpg.connect(dsn)
    try:
        async with probe.transaction():
            lock_free: bool = await probe.fetchval(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))",
                schema_lock_name("cron", schema),
            )
        assert lock_free is True, (
            "the cron advisory lock is still held after the tick ended — the "
            "transaction end that ends the tick must release it for the next worker"
        )
        async with probe.transaction():
            fired: int = await tick_cron(
                probe, settings, make_backend(settings), schema, new_uuid()
            )
    finally:
        await probe.close()
    assert fired == 1, (
        "a second worker's tick must fire a fresh due schedule once the first "
        "worker's tick has ended — cron must not stay stalled fleet-wide"
    )


# ── Attack 1 — the claim's literal mechanism, through the real path ────


class TestAsyncHungFactory:
    """An async factory that never returns, driven through the leader's
    exact nesting: the tick must end near the machinery's T and free the
    lock — the bound derives from the deadline, never from the factory."""

    async def test_tick_ends_at_the_deadline_independent_of_the_hang(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One hung async factory + one healthy peer, both due; the
        leader-shaped tick must END within T + slack while the factory is
        still parked, and the cron advisory lock must be free afterwards —
        independent of a hang that has no end of its own."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema, TASKQ_DISPATCHER_COMMAND_TIMEOUT="5.0")
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="async-hung",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory="tests.test_rt_cron_hang_livelock.async_hang_factory",
        )
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="healthy-peer",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )

        backend = make_backend(settings)
        worker_id = new_uuid()

        with _async_hang() as (entered, _gate):
            started = time.monotonic()
            outcome = "returned"
            try:
                await asyncio.wait_for(
                    _run_leader_shaped_tick(clean_pg_conn, settings, backend, schema, worker_id),
                    timeout=_BOUND_S,
                )
            except (TimeoutError, asyncio.CancelledError):
                outcome = "cut by a deadline"
            elapsed = time.monotonic() - started

        assert entered.is_set(), (
            "vacuous run: the tick never reached the hung factory, so nothing "
            "here proves anything about its hang"
        )
        assert elapsed < _BOUND_S, (
            f"the leader-shaped tick over ONE hung async payload factory took {elapsed:.1f}s "
            f"to end ({outcome}) — the deadline machinery (the whole-tick "
            f"asyncio.timeout({settings.dispatcher_command_timeout}s) plus "
            f"resolve_payload's per-factory wait_for, both {_T_S}s) did not end it "
            f"independently of the factory's hang; the transaction and the cron "
            f"advisory lock stayed open the whole time: livelock"
        )

        await _assert_lock_free_and_cron_fires(
            clean_pg_conn, module_pg_schema.pg_dsn, schema, settings
        )


# ── Attack 2 — the reproduction: the sync factory the machinery cannot see ──


class TestSyncHungFactory:
    """A sync factory that blocks the loop inside the tick's transaction:
    the reproduction of the livelock — no timer can fire, the advisory
    lock is held for the whole block, every other worker returns 0."""

    async def test_tick_ends_within_the_deadline_independent_of_the_factory_block(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One sync-blocking factory + one healthy peer, both due; the
        leader-shaped tick must END within T + slack even though the
        factory blocks the very loop the deadline machinery runs on, and
        the cron advisory lock must be free afterwards. While the tick is
        wedged, a second worker's ticks (observer thread, own loop and
        connection) record the fleet-wide contention live."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema, TASKQ_DISPATCHER_COMMAND_TIMEOUT="5.0")
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        hung_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="sync-hung",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory="tests.test_rt_cron_hang_livelock.sync_hang_factory",
        )
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="healthy-peer",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )

        backend = make_backend(settings)
        worker_id = new_uuid()

        observer: dict[str, Any] = {}
        with _sync_hang() as (entered, _release):
            watcher = threading.Thread(
                target=_second_worker_ticks_while_the_first_is_wedged,
                args=(module_pg_schema.pg_dsn, schema, settings, entered, hung_id, observer),
                daemon=True,
            )
            watcher.start()
            started = time.monotonic()
            outcome = "returned"
            try:
                await asyncio.wait_for(
                    _run_leader_shaped_tick(clean_pg_conn, settings, backend, schema, worker_id),
                    timeout=_SAFETY_NET_S,
                )
            except (TimeoutError, asyncio.CancelledError):
                outcome = "cut by a deadline"
            elapsed = time.monotonic() - started
            watcher.join(timeout=_SAFETY_NET_S + 5.0)

        assert entered.is_set(), (
            "vacuous run: the tick never reached the hung factory, so nothing "
            "here proves anything about its hang"
        )
        assert elapsed < _BOUND_S, (
            f"the leader-shaped tick over ONE hung SYNC payload factory took {elapsed:.1f}s "
            f"to end ({outcome}) — its end was set by the factory's {_SYNC_BLOCK_S}s "
            f"block, not by any deadline: the whole-tick "
            f"asyncio.timeout({settings.dispatcher_command_timeout}s) and the "
            f"per-factory wait_for both live on the event loop the sync factory "
            f"froze, so neither could fire during the hang. Second worker "
            f"(observer thread, own loop and connection): tick at "
            f"+{observer.get('early_off_s', float('nan')):.1f}s into the hang fired "
            f"{observer.get('early_fired', 'n/a')} and tick at "
            f"+{observer.get('late_off_s', float('nan')):.1f}s (past every "
            f"{_BOUND_S}s bound) fired {observer.get('late_fired', 'n/a')} — 0 means "
            f"the cron advisory lock was contended: every other worker's cron tick "
            f"returned empty for the whole hang, fleet-wide. One hung payload "
            f"factory held the cron lock {_SYNC_BLOCK_S}s: livelock"
        )

        await _assert_lock_free_and_cron_fires(
            clean_pg_conn, module_pg_schema.pg_dsn, schema, settings
        )


class TestDefaultTimeoutsCollideIntoWholeTickRollback:
    """Reproduction of issue #174: at the shipped matching defaults
    (``dispatcher_command_timeout`` == ``cron_payload_factory_timeout`` ==
    5.0s), a hung payload factory does not take a per-schedule strike —
    it takes down the WHOLE tick, including every healthy schedule batched
    alongside it.

    ``resolve_payload``'s per-factory ``asyncio.wait_for`` and the leader's
    whole-tick ``asyncio.timeout`` are armed at the same instant with the
    same duration, so which one fires is a genuine race with no
    deterministic winner. The contract this test attacks is the one
    ``cron_loop.py``'s per-row ``except Exception as exc:`` (the only
    guard around ``_plan_fire``) cannot honor even in principle:
    ``asyncio.CancelledError`` — what the outer ``asyncio.timeout``
    delivers — is a ``BaseException`` in Python >= 3.8, so it is NOT
    caught there. When the outer deadline wins the race, the cancellation
    propagates straight past the per-row handler, aborts ``tick_cron``
    entirely, and the transaction rolls back: the hung schedule gets no
    named strike (contradicting docs/guides/cron.md's "increments
    consecutive_failures" claim) AND the healthy peer schedule planned in
    the same batch loses its committed, already-computed advance —
    ``next_fire_at`` stays at the old due timestamp for BOTH schedules.
    Because ``next_fire_at`` never advances, the identical batch is
    selected again on the very next tick: an unbounded livelock that
    produces zero strike telemetry, not the bounded per-factory failure
    the docs promise.

    This test drives the exact shipped defaults (no override), unlike
    every other test in this file which pins ``TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT``
    away from the collision to get a deterministic per-factory strike.
    """

    async def test_hung_factory_at_matching_defaults_rolls_back_the_healthy_peer_too(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """Two due schedules, one with a factory that hangs forever: at
        the shipped 5.0s/5.0s default collision, the tick must end (the
        whole-tick asyncio.timeout still bounds IT), but when it ends via
        cancellation neither schedule's next_fire_at has advanced — the
        healthy peer's own already-computed, uncontested advance was
        rolled back along with the hung one's failure. A correctly
        isolated per-schedule failure model would leave the healthy peer
        FIRED and its next_fire_at advanced regardless of which schedule
        hung beside it.
        """
        schema = module_pg_schema.schema_name
        # The shipped defaults: NOT overridden away from each other. This
        # is the exact collision the issue's docstring warning
        # (taskq/cron.py's _await_factory_bounded) describes and that no
        # cross-field validator or default change enforces.
        settings = cron_settings(schema)
        assert (
            settings.dispatcher_command_timeout == settings.cron_payload_factory_timeout == 5.0
        ), (
            "test premise: the shipped defaults must still collide — if this "
            "fails, a fix already decoupled the two timeouts and this "
            "reproduction is obsolete"
        )
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="async-hung-collision",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory="tests.test_rt_cron_hang_livelock.async_hang_factory",
        )
        healthy_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="healthy-peer-collision",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )

        backend = make_backend(settings)
        worker_id = new_uuid()

        with _async_hang() as (entered, _gate):
            started = time.monotonic()
            outcome = "returned"
            try:
                await asyncio.wait_for(
                    _run_leader_shaped_tick(clean_pg_conn, settings, backend, schema, worker_id),
                    timeout=_BOUND_S,
                )
            except (TimeoutError, asyncio.CancelledError):
                outcome = "cut by a deadline"
            elapsed = time.monotonic() - started

        assert entered.is_set(), (
            "vacuous run: the tick never reached the hung factory, so nothing "
            "here proves anything about the collision"
        )
        assert elapsed < _BOUND_S, (
            f"the tick took {elapsed:.1f}s to end ({outcome}) — it should still be "
            f"bounded by the whole-tick asyncio.timeout even at the colliding defaults"
        )

        healthy_row = await clean_pg_conn.fetchrow(
            f'SELECT next_fire_at, consecutive_failures FROM "{schema}".cron_schedules '  # noqa: S608
            f"WHERE id = $1",
            healthy_id,
        )
        assert healthy_row is not None
        assert healthy_row["next_fire_at"] > due, (
            "issue #174 reproduced: at the shipped matching defaults "
            "(dispatcher_command_timeout == cron_payload_factory_timeout == 5.0s), "
            "the whole-tick asyncio.timeout's CancelledError propagates past "
            "cron_loop.py's per-row `except Exception` (CancelledError is a "
            "BaseException, not caught there), aborting tick_cron entirely. The "
            f"transaction rolled back and the HEALTHY peer schedule's next_fire_at "
            f"({healthy_row['next_fire_at']}) was never advanced past its due "
            f"timestamp ({due}) even though it carried no hung factory and no error "
            "of its own — a single stuck cron factory wedges every other due "
            "schedule in its batch, indefinitely, with zero per-schedule strike "
            "telemetry (consecutive_failures stayed "
            f"{healthy_row['consecutive_failures']}). The identical batch is "
            "selected again on the next tick: livelock, not the documented "
            "per-factory failure."
        )


class TestHungFactoryStrikesAtShippedDefaults:
    """A hung payload factory must accrue a strike under the timeouts that
    actually ship, not only under a hand-tuned pair.

    Strike accounting is what eventually auto-disables a schedule nobody is
    watching: a factory that hangs on every fire is exactly the case the
    three-strike auto-disable exists for, because it costs a full deadline
    of the cron lock every tick forever. A hang that ends the tick without
    recording anything on the row leaves the schedule permanently due, with
    an empty ``last_fire_error`` and a zero failure count — the operator
    sees an idle-looking schedule and a cron loop quietly burning its
    budget. Escaping the strike must not be possible merely because the
    whole-tick deadline and the per-factory deadline happen to be equal at
    shipped defaults.
    """

    async def test_hung_factory_records_its_strike_under_shipped_timeouts(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """One due schedule whose factory hangs, run under the shipped
        defaults with no timeout override: after the tick, the row must
        carry a strike and a ``last_fire_error`` naming the factory that
        hung.
        """
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        hung_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="hung-at-shipped-defaults",
            cron_expr=_HOURLY,
            next_fire_at=due,
            payload_factory="tests.test_rt_cron_hang_livelock.async_hang_factory",
        )

        backend = make_backend(settings)
        worker_id = new_uuid()

        with (
            _async_hang() as (entered, _gate),
            contextlib.suppress(TimeoutError, asyncio.CancelledError),
        ):
            await asyncio.wait_for(
                _run_leader_shaped_tick(clean_pg_conn, settings, backend, schema, worker_id),
                timeout=_BOUND_S,
            )

        assert entered.is_set(), (
            "vacuous run: the tick never reached the hung factory, so nothing "
            "here proves anything about its strike"
        )

        row = await clean_pg_conn.fetchrow(
            f'SELECT consecutive_failures, last_fire_error FROM "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            "WHERE id = $1",
            hung_id,
        )
        assert row is not None
        assert row["consecutive_failures"] == 1, (
            "a hung payload factory must take a per-schedule strike under the "
            "timeouts that ship; it recorded "
            f"{row['consecutive_failures']}. Without it the schedule never "
            "reaches the auto-disable threshold and keeps costing a full "
            "deadline of the cron advisory lock on every tick, forever"
        )
        assert "async_hang_factory" in (row["last_fire_error"] or ""), (
            "the stored failure must name the factory that hung — it is the "
            "only thing distinguishing it from every other schedule's factory; "
            f"got {row['last_fire_error']!r}"
        )


class TestHungFactoryBatchCannotLivelockTheTick:
    """A batch full of simultaneously-due hung factories must not overrun
    the whole-tick deadline in aggregate.

    The per-factory deadline bounds ONE factory's wait. A tick's batch can
    hold up to ``cron_tick_limit`` (default 100) due schedules, and the
    planning loop awaits each factory in turn — so the BATCH's aggregate
    factory wait is what the leader's whole-tick ``asyncio.timeout``
    actually races. Any per-factory floor that lets each wait exceed the
    tick's remaining budget (even by 50 ms) sums past the whole-tick
    deadline once a dozen-plus hung factories share a batch: the outer
    deadline wins, its ``CancelledError`` aborts the tick, the transaction
    rolls back every strike, and the identical batch is re-selected on the
    very next tick — forever. The tick must instead stop funding factory
    waits once its budget is spent: the remaining factory-backed schedules
    take an immediate, named strike and the tick commits inside its
    deadline.
    """

    async def test_a_batch_of_hung_factories_still_commits_strikes_and_fires_the_peer(
        self,
        clean_pg_conn: asyncpg.Connection,
        module_pg_schema: ModulePgSchema,
    ) -> None:
        """20 due schedules on one hung factory plus one healthy static
        peer: the leader-shaped tick must RETURN inside its own deadline
        (not be cut by it), every hung schedule must carry a committed
        strike (the path to auto-disable), the schedules the budget could
        not fund must name the budget exhaustion as their failure reason,
        and the healthy peer must have fired and advanced."""
        schema = module_pg_schema.schema_name
        settings = cron_settings(schema)
        await seed_actor_config(clean_pg_conn, schema, _ACTOR)
        due = hour_floor(datetime.now(UTC))
        hung_count = 20
        hung_ids = [
            await seed_schedule(
                clean_pg_conn,
                schema,
                actor=_ACTOR,
                name=f"hung-batch-{i}",
                cron_expr=_HOURLY,
                next_fire_at=due,
                payload_factory="tests.test_rt_cron_hang_livelock.async_hang_factory",
            )
            for i in range(hung_count)
        ]
        healthy_id = await seed_schedule(
            clean_pg_conn,
            schema,
            actor=_ACTOR,
            name="healthy-peer-batch",
            cron_expr=_HOURLY,
            next_fire_at=due,
        )

        backend = make_backend(settings)
        worker_id = new_uuid()

        with _async_hang() as (entered, _gate):
            started = time.monotonic()
            outcome = "returned"
            fired: int | None = None
            try:
                fired = await asyncio.wait_for(
                    _run_leader_shaped_tick(clean_pg_conn, settings, backend, schema, worker_id),
                    timeout=_BOUND_S,
                )
            except (TimeoutError, asyncio.CancelledError):
                outcome = "cut by a deadline"
            elapsed = time.monotonic() - started

        assert entered.is_set(), (
            "vacuous run: the tick never reached the hung factory, so nothing "
            "here proves anything about the batch"
        )
        assert fired is not None and outcome == "returned", (
            f"the tick over {hung_count} hung factories did not return — it was "
            f"{outcome} after {elapsed:.1f}s. The per-factory waits summed past "
            "the whole-tick deadline, the outer asyncio.timeout's CancelledError "
            "aborted tick_cron, and the transaction rolled back every strike: "
            "the identical batch is selected again next tick — livelock"
        )
        assert elapsed < _BOUND_S
        assert fired == 1, "exactly the healthy static-peer schedule fired"

        hung_rows = [
            await schedule_row(clean_pg_conn, schema, schedule_id) for schedule_id in hung_ids
        ]
        for schedule_id, row in zip(hung_ids, hung_rows, strict=True):
            assert row["consecutive_failures"] == 1, (
                f"schedule {schedule_id} recorded {row['consecutive_failures']} "
                "strikes — a rolled-back tick leaves the whole batch at zero, "
                "so auto-disable is unreachable and the livelock has no telemetry"
            )
        budget_exhausted = [
            row for row in hung_rows if "budget exhausted" in (row["last_fire_error"] or "")
        ]
        assert len(budget_exhausted) >= hung_count - 2, (
            f"only {len(budget_exhausted)} of {hung_count} hung schedules were "
            "failed immediately with a tick-budget-exhausted reason — at most "
            "two real factory waits can fit inside the tick's funded budget, "
            "so every later schedule must be struck without waiting; waiting "
            "for each is exactly the aggregate overrun that cancels the tick"
        )

        healthy_row = await clean_pg_conn.fetchrow(
            f'SELECT next_fire_at FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
            healthy_id,
        )
        assert healthy_row is not None
        assert healthy_row["next_fire_at"] > due, (
            "the healthy peer's advance was rolled back with the aborted tick — "
            "a hung batch must not take down schedules that carry no factory"
        )


class TestFactoryDeadlineIsTunable:
    """The per-factory deadline is a setting that reaches the factory call.

    The whole-tick deadline (``dispatcher_command_timeout``) and the
    per-factory deadline were both 5.0 s by coincidence of defaults: an
    operator tuning the whole-tick deadline below 5.0 s would leave a
    per-factory budget that silently exceeds it, so the leader's
    whole-tick ``asyncio.timeout`` cancels mid-factory and the named
    per-schedule failure this deadline exists to record is lost. The
    setting keeps the two budgets coherent by making the inner one
    tunable — and the deadline must actually reach the factory call.
    """

    def test_cron_payload_factory_timeout_is_a_worker_setting(self) -> None:
        """The field exists, defaults to the historical 5.0 s, and loads
        from the TASKQ_ env prefix."""
        from taskq.settings import WorkerSettings

        entry = WorkerSettings.get_fields().get("cron_payload_factory_timeout")
        assert entry is not None, (
            "cron_payload_factory_timeout is missing from WorkerSettings — the "
            "per-factory deadline is a hardcoded constant an operator cannot "
            "keep below the whole-tick deadline they DID tune."
        )
        _type, info = entry
        assert info.default == 5.0, (
            f"defaults to {info.default!r}, not the historical 5.0 s — wiring "
            "the knob must preserve today's deadline or every deployment that "
            "does not set the env var changes behavior on upgrade."
        )
        loaded = WorkerSettings.load_from_dict(
            {
                "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
                "TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT": "0.5",
            },
            validate=False,
        )
        assert loaded.cron_payload_factory_timeout == 0.5

    async def test_resolve_payload_timeout_s_bounds_the_factory_deadline(self) -> None:
        """An explicit ``timeout_s`` cuts the factory call at THAT value
        (not the 5.0 s default) and the error names the effective
        deadline — the only place an operator sees which budget fired."""
        from taskq.cron import resolve_payload

        t0 = time.monotonic()
        with pytest.raises(TimeoutError) as exc_info:
            await asyncio.wait_for(
                resolve_payload(
                    "tests.test_rt_cron_harness.hang_past_factory_timeout",
                    {},
                    timeout_s=0.1,
                ),
                timeout=5.0,
            )
        elapsed = time.monotonic() - t0

        assert "timed out after 0.1s" in str(exc_info.value), (
            f"the timeout must name the EFFECTIVE deadline (0.1s), got: {exc_info.value!r}"
        )
        assert elapsed < 5.0, "the 0.1 s override did not bound the call"

    async def test_cron_loop_resolve_payload_forwards_the_deadline(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The cron tick's wrapper forwards its caller's deadline to the
        shared resolver — the seam ``_plan_fire`` uses to pass the
        setting."""
        from taskq.worker import cron_loop as cron_loop_mod

        forwarded: dict[str, float | None] = {}

        async def _recording_resolver(
            payload_factory: str | None, raw_metadata: object, *, timeout_s: float | None = None
        ) -> dict[str, object]:
            forwarded["timeout_s"] = timeout_s
            return {}

        monkeypatch.setattr(cron_loop_mod, "resolve_cron_payload", _recording_resolver)
        await cron_loop_mod.resolve_payload(
            {"payload_factory": "some.factory", "metadata": {}}, timeout_s=0.25
        )
        assert forwarded["timeout_s"] == 0.25, (
            "the wrapper dropped the deadline — the setting never reaches the factory call"
        )
