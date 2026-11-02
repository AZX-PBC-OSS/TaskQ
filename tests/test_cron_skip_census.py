"""The census attack: every drop path, the exact count the operator sees.

The claim under attack (``tests/test_cron_skipped_slots.py`` pins the happy
shape) is that ``taskq.cron.skipped_slots`` and ``taskq.cron.slots_behind``
count EVERY dropped cron slot exactly once: no double-count across
rollbacks and leader failovers, no silent loss on the suppression paths,
and a gauge that decays but never lies. This file builds the census path
by path and pins the operator-visible count for each:

1. committed catch-up skip — counted once, AFTER the commit gate delivers
   (the crash window between the tick's last statement and the emission
   is the gate's; a crash there loses the count, the at-most-once bound);
2. a ROLLED-BACK tick (commit never happened) — reports NOTHING, the owed
   slots are re-attempted, and the committed re-attempt counts them
   exactly ONCE (never doubled); a stale nonce from the rolled-back arm
   cannot re-emit;
3. leader failover mid-tick — the old leader's rollback reports nothing,
   the new leader's committed re-fire counts once;
4. budget deferral, committed — counted (the suppression UPDATE durably
   advances past the owed slots); budget deferral rolled back — nothing,
   re-attempted, counted once on the commit;
5. the auto-disable boundary — a failing schedule's strikes count
   NOTHING (its UPDATE never advances ``next_fire_at``, the slots are
   re-attempted) right through the disable itself.

Then the gauge's decay honesty: merge-not-replace across a tick that
observes only DUE schedules, the DELETED-schedule strand (the one
unbounded-strand path), and the 101st actor's collapse onto ``_other_``
on BOTH instruments.

Then the fire-units math: a mid-lag period change, a DST fold and a DST
gap, and the exactness of the catch-up cutoff boundary.

Finally the mutation pin: the runaway shape's fires-survive contract must
stay RED against a change that "fixes" the drop by re-queuing dropped
history as jobs.

Pure-Python against the recording fakes; the real-PG proof that the gate
itself (a real NOTIFY riding a real COMMIT) gates these two instruments
lives in ``tests/test_rt_cron_skip_commit_gate.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import structlog.testing
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid
from taskq.backend._protocol import JobFilter
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.otel import counter_data_points, counter_value
from taskq.worker import cron_loop

from . import test_cron_loop as cron_loop_tests
from .test_cron_loop import (
    _NOW,
    _FakeCronConn,
    _make_actor_config_row,
    _make_schedule_row,
    _SteppableMonotonic,
    _success_updates,
    _tick,
)
from .test_cron_skipped_slots import (
    _gauge_depths,
    _overdue_row,
    _runaway_settings,
)

_SKIP_COUNTER = "taskq.cron.skipped_slots"
_SLOTS_BEHIND_GAUGE = "taskq.cron.slots_behind"


@pytest.fixture
def metric_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation, the ``test_cron_skipped_slots`` fixture.

    The skip counter resolves through :func:`taskq.obs._otel._lazy_counter`
    at call time, so patching ``get_meter`` is enough. The lag gauge is a
    module-level observable gauge bound at import time; the test re-registers
    the SAME production callback on a test-scoped meter over a fresh cache.
    Duplicated here (rather than imported) so the fixture parameters of this
    module's tests bind their OWN definition, not an imported name.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME, otel_mod._version()
    )
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: meter)
    monkeypatch.setattr(otel_mod, "_cron_slots_behind_cache", {})
    monkeypatch.setattr(
        otel_mod,
        "_cron_slots_behind_gauge",
        meter.create_observable_gauge(
            _SLOTS_BEHIND_GAUGE,
            callbacks=[otel_mod._observe_cron_slots_behind],
        ),
    )
    return reader


# ── the gated harness: a cron fake whose telemetry the COMMIT gates ────


class _GatedCronConn(_FakeCronConn):
    """A :class:`_FakeCronConn` that can carry the commit gate.

    ``_FakeCronConn`` cannot ``get_server_pid``, so every tick driven
    against it falls into :func:`cron_loop._emit_on_commit`'s INLINE
    fallback — the emission runs inside the still-uncommitted tick and
    the gate is never exercised. This subclass carries the gate: the arm
    records the ``pg_notify``, and the test decides whether the COMMIT
    answers it (:meth:`deliver_commit_notify`) or the tick rolls back
    (never delivered; :meth:`die` models the session dying with the
    armed emission unanswered). Implausible pids, so the process-global
    gate maps never collide with a real session's entry.
    """

    def __init__(
        self,
        *,
        pid: int,
        schedule_rows: list[_FakeCronConn] | None = None,
        actor_config_rows: list[_FakeCronConn] | None = None,
    ) -> None:
        super().__init__(
            schedule_rows=schedule_rows or [], actor_config_rows=actor_config_rows or []
        )  # type: ignore[arg-type]  # Why: the rows are _FakeCronRecord; the base's annotation is looser than its use.
        self.pid = pid
        self.channel_listeners: dict[str, Callable[..., None]] = {}
        self.notify_calls: list[tuple[str, str]] = []
        self.termination_listeners: list[Callable[[object], None]] = []

    def get_server_pid(self) -> int:
        return self.pid

    async def remove_listener(self, channel: str, callback: object) -> None:
        self.channel_listeners.pop(channel, None)

    async def add_listener(self, channel: str, callback: Callable[..., None]) -> None:
        self.channel_listeners[channel] = callback

    def add_termination_listener(self, callback: Callable[[object], None]) -> None:
        self.termination_listeners.append(callback)

    async def execute(self, sql: str, *args: object) -> str:
        if "pg_notify" in sql:
            self.notify_calls.append((str(args[0]), str(args[1])))
            return "SELECT 1"
        return await super().execute(sql, *args)

    def deliver_commit_notify(self) -> None:
        """The server's answer when the arming tick's COMMIT succeeds."""
        channel, nonce = self.notify_calls[-1]
        self.channel_listeners[channel](self, self.pid, channel, nonce)

    def deliver_stale_nonce(self, channel: str, nonce: str) -> None:
        """A notification from an arm the current tick superseded."""
        self.channel_listeners[channel](self, self.pid, channel, nonce)

    def die(self) -> None:
        """What asyncpg's ``_cleanup`` runs on close/terminate/loss."""
        for callback in self.termination_listeners:
            callback(self)


@pytest.fixture
def gate_maps(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clean process-global gate maps for the implausible test pids."""
    monkeypatch.setattr(cron_loop, "_armed_commit_emits", {})
    monkeypatch.setattr(cron_loop, "_confirmed_listening", set())
    monkeypatch.setattr(cron_loop, "_termination_hooked", set())


def _gated_overdue_conn(
    *,
    actor: str = "runaway_actor",
    schedule_id: UUID | None = None,
    pid: int = 2**30 + 7,
) -> _GatedCronConn:
    return _GatedCronConn(
        pid=pid,
        schedule_rows=[_overdue_row(actor=actor, schedule_id=schedule_id)],
        actor_config_rows=[_make_actor_config_row(actor=actor)],
    )


def _first_success_next_fire(conn: _FakeCronConn) -> datetime:
    """The next_fire_at the tick's one success UPDATE advanced, typed."""
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    next_fires = success_updates[0][1][1]
    assert isinstance(next_fires, list)
    first = next_fires[0]
    assert isinstance(first, datetime)
    return first


# ── census path 1: the committed skip counts once, behind the gate ─────


async def test_the_committed_skip_counts_once_behind_the_commit_gate(
    metric_reader: InMemoryMetricReader,
    gate_maps: None,
) -> None:
    """The crash-window census, pinned precisely.

    ``tick_cron`` has executed every statement of the tick (the success
    UPDATE that durably advances ``next_fire_at`` past the owed slots is
    in the connection's write log) yet the counter and the gauge have
    BOTH not moved: the emission is armed behind the commit gate, not
    inline. Delivery — the server's NOTIFY riding the COMMIT — is what
    counts the drop. A process that dies in that window (after the
    server committed, before the dispatch ran) loses the count while the
    committed advance stands: the counter is AT-MOST-ONCE per drop, and
    this pin is where that bound lives. (The metrics plane is
    process-local Prometheus state anyway: a dead process exports
    nothing further.)
    """
    conn = _gated_overdue_conn()
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    async with conn.transaction():
        fired = await _tick(conn, settings, backend)

    assert fired == 1
    # Every statement ran, the emission is armed ...
    assert len(_success_updates(conn)) == 1
    assert conn.notify_calls, "the arm must have issued the gate's pg_notify"
    assert conn.pid in cron_loop._armed_commit_emits, (
        "the emission waits on the commit gate - a crash between the last "
        "statement and the dispatch must not have counted anything yet"
    )
    # ... but nothing has been counted.
    assert counter_value(metric_reader, _SKIP_COUNTER) == 0, (
        "the drop must not count before the commit gate delivers"
    )
    assert _gauge_depths(metric_reader) == {}

    conn.deliver_commit_notify()

    assert counter_value(metric_reader, _SKIP_COUNTER) == 3
    assert _gauge_depths(metric_reader) == {"runaway_actor": 3}


# ── census path 2: a rolled-back tick reports nothing, counted once ────


async def test_a_rolled_back_tick_reports_nothing_and_the_reattempt_counts_once(
    metric_reader: InMemoryMetricReader,
    gate_maps: None,
) -> None:
    """The census's no-double-count half.

    Tick 1 plans the fire, executes the success UPDATE, arms the
    emission — and the COMMIT never happens (the transaction rolls back:
    a transient PG error, the leader's deadline, anything). The owed
    slots were NOT dropped by that tick — ``next_fire_at`` rolled back
    with everything else — so the counter must say nothing. The
    re-attempted tick then commits and counts the drops ONCE: a census
    that summed both attempts would report 6 dropped slots for a
    schedule that lost exactly 3.
    """
    conn = _gated_overdue_conn(schedule_id=new_uuid())
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    async with conn.transaction():
        await _tick(conn, settings, backend)
    # The rollback: the gate is never answered. (The fake's rows are
    # static, so the re-attempt selects the identical owed row — exactly
    # what a rolled-back next_fire_at produces on a real database.)
    assert counter_value(metric_reader, _SKIP_COUNTER) == 0

    async with conn.transaction():
        await _tick(conn, settings, backend)
    conn.deliver_commit_notify()

    assert counter_value(metric_reader, _SKIP_COUNTER) == 3, (
        "the re-attempted tick counts the drop exactly once - the "
        "rolled-back attempt's armed emission must have been superseded, "
        "not stacked"
    )
    assert _gauge_depths(metric_reader) == {"runaway_actor": 3}


async def test_a_stale_nonce_from_the_rolled_back_arm_cannot_double_emit(
    metric_reader: InMemoryMetricReader,
    gate_maps: None,
) -> None:
    """The nonce mechanism is WHAT makes the re-attempt count once: a
    rolled-back tick's armed emission is a notification no server will
    ever send. Delivering that stale arm's nonce anyway (the shape of a
    gate whose bookkeeping leaked) must be ignored — the emission was
    replaced, not queued."""
    conn = _gated_overdue_conn(schedule_id=new_uuid())
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    async with conn.transaction():
        await _tick(conn, settings, backend)
    stale_channel, stale_nonce = conn.notify_calls[-1]

    async with conn.transaction():
        await _tick(conn, settings, backend)
    conn.deliver_commit_notify()
    assert counter_value(metric_reader, _SKIP_COUNTER) == 3

    conn.deliver_stale_nonce(stale_channel, stale_nonce)

    assert counter_value(metric_reader, _SKIP_COUNTER) == 3, (
        "a superseded arm's nonce must not re-run the emission - the "
        "rolled-back attempt stays uncounted even if its notification "
        "were somehow delivered"
    )


# ── census path 3: leader failover mid-tick counts once ────────────────


async def test_leader_failover_mid_tick_counts_the_drop_exactly_once(
    metric_reader: InMemoryMetricReader,
    gate_maps: None,
) -> None:
    """The failover census: the old leader plans the fire, executes its
    writes, arms the emission — and loses its session (rollback, the
    lock releases, a new leader is elected). The new leader re-fires the
    same owed slots from its own session and its commit counts them
    once. The old leader's session death must retire its armed emission
    (it can never be answered), so failover cannot double-count the
    drop, and the old session's stale arm cannot outlive it."""
    old = _gated_overdue_conn(schedule_id=new_uuid(), pid=2**30 + 21)
    new = _gated_overdue_conn(schedule_id=new_uuid(), pid=2**30 + 22)
    settings = _runaway_settings()

    old_backend = InMemoryBackend(clock=FakeClock(_NOW))
    new_backend = InMemoryBackend(clock=FakeClock(_NOW))

    async with old.transaction():
        await _tick(old, settings, old_backend)
    # The old leader's transaction rolled back and its session died: the
    # armed emission is retired by the termination hook, never answered.
    old.die()
    assert old.pid not in cron_loop._armed_commit_emits
    assert counter_value(metric_reader, _SKIP_COUNTER) == 0, (
        "the failed-over leader's uncommitted attempt reports nothing"
    )

    async with new.transaction():
        await _tick(new, settings, new_backend)
    new.deliver_commit_notify()

    assert counter_value(metric_reader, _SKIP_COUNTER) == 3, (
        "the new leader's committed re-fire counts the drop once - the "
        "old leader's rollback must not have stacked a second count"
    )
    assert _gauge_depths(metric_reader) == {"runaway_actor": 3}


# ── census path 4: budget deferral, committed and rolled back ──────────


async def test_a_budget_deferred_suppression_counts_only_once_it_commits(
    metric_reader: InMemoryMetricReader,
    gate_maps: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The budget-deferral census, both halves.

    Tick 1: the consumer fires and eats the funded grant; the OVERDUE
    schedule behind it defers (its ``skipped_slots`` were already
    computed in planning) and the suppression UPDATE durably advances it
    past the owed slots. The COMMIT fails. Nothing is counted: the
    rolled-back suppression did NOT durably advance anything — the owed
    slots are re-attempted.

    Tick 2: with budget to fund both factories, the consumer fires (2
    dropped hourly slots) and the deferred schedule fires (3 dropped
    */5 slots). Each actor's census is EXACTLY once, despite the
    rolled-back attempt having planned the very same counts.
    """
    consumer_id, deferred_id = new_uuid(), new_uuid()
    conn = _GatedCronConn(
        pid=2**30 + 31,
        schedule_rows=[
            _overdue_row(
                actor="consumer_actor",
                schedule_id=consumer_id,
                overdue=timedelta(minutes=15),
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._full_grant_unit_factory",
            ),
            _overdue_row(
                actor="deferred_runaway_actor",
                schedule_id=deferred_id,
                overdue=timedelta(minutes=10),
                cron_expr="*/5 * * * *",
                payload_factory="tests.test_cron_loop._fast_unit_factory",
            ),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="consumer_actor"),
            _make_actor_config_row(actor="deferred_runaway_actor"),
        ],
    )
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    fake_time = _SteppableMonotonic(100.0)

    async def _grant_consuming_resolver(
        row: object, *, timeout_s: float | None = None
    ) -> dict[str, object]:
        fake_time.advance(timeout_s or 0.0)
        return {}

    monkeypatch.setattr(cron_loop, "resolve_payload", _grant_consuming_resolver)
    monkeypatch.setattr(cron_loop, "time", fake_time)

    async with conn.transaction():
        fired = await _tick(conn, settings, backend)

    assert fired == 1, "tick 1: the consumer fires; the overdue peer defers"
    # The suppression UPDATE ran — the crash-window pin for the deferral
    # path: the write happened, the count has not.
    suppressions = [
        sql for sql, _args in conn.execute_calls if "SET next_fire_at = f.next_fire" in sql
    ]
    assert len(suppressions) == 1
    assert counter_value(metric_reader, _SKIP_COUNTER) == 0, (
        "the rolled-back deferral counts nothing - its suppression UPDATE "
        "rolled back, the owed slots were not dropped by that tick"
    )
    assert _gauge_depths(metric_reader) == {}

    # Tick 2, budget healthy: both fire and commit. Re-attempted, counted
    # once each.
    async def _fast_resolver(row: object, *, timeout_s: float | None = None) -> dict[str, object]:
        return {}

    monkeypatch.setattr(cron_loop, "resolve_payload", _fast_resolver)
    async with conn.transaction():
        fired = await _tick(conn, settings, backend)
    assert fired == 2
    conn.deliver_commit_notify()

    by_actor = {
        dict(p.attributes or {})["actor"]: p.value
        for p in counter_data_points(metric_reader, _SKIP_COUNTER)
    }
    assert by_actor.get("consumer_actor") == 2
    assert by_actor.get("deferred_runaway_actor") == 3, (
        "the committed re-attempt counts each actor's drop exactly once - "
        "the rolled-back deferral's planned count must not have stacked"
    )
    assert _gauge_depths(metric_reader) == {"consumer_actor": 2, "deferred_runaway_actor": 3}


async def test_a_committed_budget_deferral_counts_its_skips_from_the_suppression_alone(
    metric_reader: InMemoryMetricReader,
    gate_maps: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The census row for the committed deferral, on the gated path, with
    the suppression as the ONLY possible source of the count: the
    deferred schedule NEVER fires — its owed slots are dropped purely by
    the committed suppression UPDATE advancing ``next_fire_at`` past
    them. If the suppression stopped carrying its ``skipped_slots``, the
    drops would vanish from the metrics plane (the one silent-loss hole
    the fix closed)."""
    consumer_id, deferred_id = new_uuid(), new_uuid()
    conn = _GatedCronConn(
        pid=2**30 + 41,
        schedule_rows=[
            _overdue_row(
                actor="consumer_actor",
                schedule_id=consumer_id,
                overdue=timedelta(minutes=15),
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._full_grant_unit_factory",
            ),
            _overdue_row(
                actor="deferred_runaway_actor",
                schedule_id=deferred_id,
                overdue=timedelta(minutes=10),
                cron_expr="*/5 * * * *",
                payload_factory="tests.test_cron_loop._fast_unit_factory",
            ),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="consumer_actor"),
            _make_actor_config_row(actor="deferred_runaway_actor"),
        ],
    )
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    fake_time = _SteppableMonotonic(100.0)

    async def _grant_consuming_resolver(
        row: object, *, timeout_s: float | None = None
    ) -> dict[str, object]:
        fake_time.advance(timeout_s or 0.0)
        return {}

    monkeypatch.setattr(cron_loop, "resolve_payload", _grant_consuming_resolver)
    monkeypatch.setattr(cron_loop, "time", fake_time)

    async with conn.transaction():
        fired = await _tick(conn, settings, backend)
    assert fired == 1
    conn.deliver_commit_notify()

    by_actor = {
        dict(p.attributes or {})["actor"]: p.value
        for p in counter_data_points(metric_reader, _SKIP_COUNTER)
    }
    assert by_actor.get("deferred_runaway_actor") == 3, (
        "the deferred schedule never fired - its three dropped slots can "
        "only have come from the committed suppression's carried count; "
        "if this reads 0 (or is missing), the budget-deferral drops are "
        "silently lost again"
    )
    assert by_actor.get("consumer_actor") == 2
    assert _gauge_depths(metric_reader) == {
        "consumer_actor": 2,
        "deferred_runaway_actor": 3,
    }, "the gauge takes the suppression's depth too, not just the counter's"


# ── census path 5: the auto-disable boundary counts nothing ────────────


async def test_the_auto_disable_boundary_counts_no_skip(
    metric_reader: InMemoryMetricReader,
) -> None:
    """A failing schedule whose failures run it to auto-disable, OVERDUE
    the whole way: the strikes count nothing (a failing fire's UPDATE
    never advances ``next_fire_at``, so the owed slots are re-attempted,
    not dropped), through the disable itself. The disable ends the
    schedule's story without minting a skip count — the census verdict
    for this path is ZERO, the disable signal itself being the louder
    operator fact."""
    settings = _runaway_settings()  # threshold 3
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    for attempt, consecutive in enumerate((0, 1, 2), start=1):
        conn = _FakeCronConn(
            schedule_rows=[
                _make_schedule_row(
                    actor="dying_actor",
                    consecutive_failures=consecutive,
                    payload_factory="nonexistent.module.fn",
                    next_fire_at=_NOW - timedelta(minutes=10),
                    schedule_id=new_uuid(),
                )
            ],
            actor_config_rows=[_make_actor_config_row(actor="dying_actor")],
        )
        with structlog.testing.capture_logs() as logs:
            fired = await _tick(conn, settings, backend)
        assert fired == 0, f"attempt {attempt}: a failing factory fires nothing"
        assert counter_value(metric_reader, _SKIP_COUNTER) == 0, (
            f"attempt {attempt}: the overdue slots were re-attempted, not "
            "dropped - a strike counts no skip"
        )
        disabled_event = "cron schedule auto-disabled" if consecutive == 2 else "cron fire failed"
        assert [e for e in logs if e["event"] == disabled_event], (
            f"attempt {attempt}: the strike logs {disabled_event!r}"
        )

    # The third strike auto-disabled the schedule. The census verdict
    # stands: the disable is the signal, the skipped-slot counter says 0.
    assert counter_value(metric_reader, _SKIP_COUNTER) == 0


# ── the gauge's decay honesty ──────────────────────────────────────────


async def test_the_gauge_merge_keeps_an_absent_actors_depth(
    metric_reader: InMemoryMetricReader,
) -> None:
    """Merge-not-replace, pinned as CORRECT for the sampled-observation
    shape: a tick observes only DUE schedules, so an actor that is not
    due this tick keeps its last observed depth instead of flapping out
    of the series set. A clean fire from a NEIGHBOUR actor must not
    decay the absent actor's depth — only the absent actor's own next
    observation can."""
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    behind_conn = _FakeCronConn(
        schedule_rows=[_overdue_row(actor="absent_next_actor", schedule_id=new_uuid())],
        actor_config_rows=[_make_actor_config_row(actor="absent_next_actor")],
    )
    await _tick(behind_conn, settings, backend)
    assert _gauge_depths(metric_reader) == {"absent_next_actor": 3}

    # Now the fleet rotates: a different actor is due and fires clean;
    # the behind schedule is NOT in the due set.
    clean_conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="other_actor", next_fire_at=_NOW),
        ],
        actor_config_rows=[_make_actor_config_row(actor="other_actor")],
    )
    await _tick(clean_conn, settings, backend)

    assert _gauge_depths(metric_reader) == {"absent_next_actor": 3, "other_actor": 0}, (
        "merge-not-replace: the unobserved actor keeps its last known "
        "depth rather than flapping out of the series set"
    )


async def test_a_deleted_schedule_strands_its_gauge_depth(
    metric_reader: InMemoryMetricReader,
) -> None:
    """The strand probe. The docs' decay story — 'a clean fire
    re-observes the actor at 0' — presupposes a next fire. A schedule
    that is DELETED while behind never fires again, never re-observes,
    and the depth gauge has no table reconcile (unlike
    ``taskq.cron.consecutive_failures``, which re-aggregates the whole
    table every tick). The strand is real and bounded by nothing: this
    pin documents it as the gauge's one unbounded-strand path, so the
    day someone adds the reconcile, this test is what goes red."""
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    behind_conn = _FakeCronConn(
        schedule_rows=[_overdue_row(actor="deleted_actor", schedule_id=new_uuid())],
        actor_config_rows=[_make_actor_config_row(actor="deleted_actor")],
    )
    await _tick(behind_conn, settings, backend)
    assert _gauge_depths(metric_reader) == {"deleted_actor": 3}

    # The schedule is deleted: its actor never appears in a due set
    # again. Two quiet ticks with other work, the pattern the reconcile
    # for the failures gauge uses to self-correct.
    for _ in range(2):
        clean_conn = _FakeCronConn(
            schedule_rows=[
                _make_schedule_row(actor="survivor_actor", next_fire_at=_NOW),
            ],
            actor_config_rows=[_make_actor_config_row(actor="survivor_actor")],
        )
        await _tick(clean_conn, settings, backend)

    assert _gauge_depths(metric_reader) == {"deleted_actor": 3, "survivor_actor": 0}, (
        "STRANDED: the deleted schedule's depth has no reconcile path - "
        "the merge-not-replace cache keeps it outliving its row forever. "
        "If this assertion starts failing, a deletion reconcile landed; "
        "until then the runbook's 'returns to 0 on its next clean fire' "
        "recovery step silently does not cover deletion"
    )


async def test_the_101st_actors_drops_collapse_onto_other_on_both_instruments(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cap's honesty, both instruments: past 100 distinct actor
    names the drop COUNT collapses onto ``_other_`` summed (honest — the
    operator still sees every dropped slot), and the depth gauge
    collapses onto ``_other_`` LAST-OBSERVATION-WINS (the one dishonest
    corner: several overflow actors' depths take turns on one series).
    Pinned as shipped, so a change to either behavior is a decision."""
    monkeypatch.setattr(otel_mod, "_cron_actor_label_values", set())
    cap = otel_mod._MAX_ACTOR_LABEL_VALUES
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    # Fill the cap: 100 distinct actors, each dropping 3 slots.
    for i in range(cap):
        conn = _FakeCronConn(
            schedule_rows=[_overdue_row(actor=f"tenant-actor-{i}", schedule_id=new_uuid())],
            actor_config_rows=[_make_actor_config_row(actor=f"tenant-actor-{i}")],
        )
        await _tick(conn, settings, backend)

    # Two overflow actors with DIFFERENT depths: 3, then 6.
    for overdue_minutes, _depth in ((10, 3), (25, 6)):
        conn = _FakeCronConn(
            schedule_rows=[
                _overdue_row(
                    actor="overflow_actor",
                    schedule_id=new_uuid(),
                    overdue=timedelta(minutes=overdue_minutes),
                )
            ],
            actor_config_rows=[_make_actor_config_row(actor="overflow_actor")],
        )
        await _tick(conn, settings, backend)

    by_actor = {
        dict(p.attributes or {})["actor"]: p.value
        for p in counter_data_points(metric_reader, _SKIP_COUNTER)
    }
    assert by_actor.get("_other_") == 3 + 6, (
        "the counter's _other_ SUMS the overflow drops - every dropped "
        "slot stays visible past the cap"
    )
    depths = _gauge_depths(metric_reader)
    assert depths.get("_other_") == 6, (
        "the gauge's _other_ is the most recent overflow observation "
        "(last-writer-wins, 6 here) - the depth of the overflow actors "
        "takes turns on one series"
    )


# ── the fire-units math ────────────────────────────────────────────────


async def test_a_mid_lag_period_change_counts_in_current_expression_units(
    metric_reader: InMemoryMetricReader,
) -> None:
    """A schedule falls behind under ``*/5``; mid-lag the row's
    expression is updated to ``*/10`` (an operator widening the period —
    exactly the knob the runbook names). The skip walk has only the
    CURRENT row: it counts the occurrences the CURRENT expression would
    have had between the owed slot and the recomputed fire. Owed 30
    minutes back, recomputed to 10:10: four */10 slots dropped — NOT the
    seven the historical */5 grid would have owed. The count is defined
    by the row the tick can see; historical-period slots are not
    replayable from it. Pinned so the semantics are a decision, not an
    accident."""
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="widened_actor",
                cron_expr="*/10 * * * *",
                next_fire_at=_NOW - timedelta(minutes=30),
                schedule_id=new_uuid(),
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="widened_actor")],
    )
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fired = await _tick(conn, settings, backend)

    assert fired == 1
    assert counter_value(metric_reader, _SKIP_COUNTER) == 4, (
        "four */10 occurrences (09:40, 09:50, 10:00, and the hop onto "
        "10:10) - the walk counts in the CURRENT expression's units"
    )
    assert _gauge_depths(metric_reader) == {"widened_actor": 4}
    # The advance itself uses the new expression too: the recomputed
    # fire (10:10) is what the immediate fire delivers; the advance
    # lands on the NEXT slot after it: 10:20 on the new grid.
    assert _first_success_next_fire(conn) == datetime(2025, 1, 1, 10, 20, tzinfo=UTC)


async def test_a_dst_fold_counts_wall_fire_units_and_the_repeated_hour_is_one_slot(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fold probe (America/New_York, 2025-11-02, 02:00 EDT → 01:00
    EST): an hourly-at-:30 schedule owed 00:30 EDT, leader alive at 01:30
    EST. The UTC span owed→recomputed is three hours, but the count is
    TWO fire units — 00:30 and the fold-0 01:30 — because under ``skip``
    the repeated hour plays ONCE: the fold-1 replay of 01:30 is not a
    dropped slot, and the count must not inflate by the fold. The
    runaway predicate compares fire units against the period, so a fold
    may never mint a phantom drop."""
    fold_now = datetime(2025, 11, 2, 6, 30, 0, tzinfo=UTC)  # 01:30 EST
    # The fake conn reads its planning clock from the shared _NOW.
    monkeypatch.setattr(cron_loop_tests, "_NOW", fold_now)

    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="fold_actor",
                cron_expr="30 * * * *",
                timezone="America/New_York",
                next_fire_at=datetime(2025, 11, 2, 4, 30, 0, tzinfo=UTC),  # 00:30 EDT
                schedule_id=new_uuid(),
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="fold_actor")],
    )
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(fold_now))

    with structlog.testing.capture_logs() as logs:
        fired = await _tick(conn, settings, backend)

    assert fired == 1
    warning = next(e for e in logs if e["event"] == "cron missed slots skipped")
    assert warning["skipped_slots"] == 2, (
        "two dropped wall-clock fires (00:30 EDT, 01:30 fold-0) across a "
        "three-hour UTC span - the fold-1 replay is not a drop"
    )
    assert counter_value(metric_reader, _SKIP_COUNTER) == 2
    assert _gauge_depths(metric_reader) == {"fold_actor": 2}
    # The advance lands strictly past the fold: the recomputed fire is
    # 02:30 EST (delivered by the immediate fire), the next owed slot is
    # 03:30 EST - the first slot entirely beyond the repeated hour.
    assert _first_success_next_fire(conn) == datetime(2025, 11, 2, 8, 30, 0, tzinfo=UTC)


async def test_a_dst_gap_collapses_nonexistent_wall_slots_into_real_ones(
    metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gap probe (America/New_York, 2025-03-09, 02:00 EST → 03:00
    EDT): a */15 schedule owed 01:45 EST, leader alive at 03:30 EDT. The
    wall slots 02:15/02:30/02:45 never existed as instants; the walk
    hops the gap and the count stays in REAL fire units — 4 (01:45,
    then the gap-resolved 03:00, 03:15, 03:30) — never a phantom count
    of slots no clock ever held. The 'hour missing' side of the fold
    changes no unit."""
    gap_now = datetime(2025, 3, 9, 7, 30, 0, tzinfo=UTC)  # 03:30 EDT
    monkeypatch.setattr(cron_loop_tests, "_NOW", gap_now)

    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="gap_actor",
                cron_expr="*/15 * * * *",
                timezone="America/New_York",
                next_fire_at=datetime(2025, 3, 9, 6, 45, 0, tzinfo=UTC),  # 01:45 EST
                schedule_id=new_uuid(),
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="gap_actor")],
    )
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(gap_now))

    fired = await _tick(conn, settings, backend)

    assert fired == 1
    assert counter_value(metric_reader, _SKIP_COUNTER) == 4
    assert _gauge_depths(metric_reader) == {"gap_actor": 4}
    # Recomputed fire 03:45 EDT delivered by the immediate fire; the
    # advance lands on 04:00 EDT.
    assert _first_success_next_fire(conn) == datetime(2025, 3, 9, 8, 0, 0, tzinfo=UTC)


async def test_the_catch_up_cutoff_boundary_is_exact(
    metric_reader: InMemoryMetricReader,
) -> None:
    """The cutoff is a cliff, pinned on both sides. A slot EXACTLY at
    ``server_now - window`` is still attempted (caught up late, counted
    as nothing); one second older it is beyond the window and the tick
    drops the whole batch of owed slots. The one-second cliff is the
    design — a window is binary — and the census must say 0 or 3, never
    anything in between."""
    settings = _runaway_settings()  # window 300s
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    at_cutoff = _FakeCronConn(
        schedule_rows=[
            _overdue_row(
                actor="boundary_actor",
                schedule_id=new_uuid(),
                overdue=timedelta(seconds=300),  # fire_at == cutoff exactly
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="boundary_actor")],
    )
    fired = await _tick(at_cutoff, settings, backend)
    assert fired == 1
    assert counter_value(metric_reader, _SKIP_COUNTER) == 0, (
        "a slot exactly AT the cutoff is attempted, not skipped - the branch is strict"
    )
    assert _first_success_next_fire(at_cutoff) == datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC), (
        "the caught-up fire advances to the next normal slot (10:05)"
    )

    one_second_older = _FakeCronConn(
        schedule_rows=[
            _overdue_row(
                actor="cliff_actor",
                schedule_id=new_uuid(),
                overdue=timedelta(seconds=301),
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="cliff_actor")],
    )
    fired = await _tick(one_second_older, settings, backend)
    assert fired == 1
    assert counter_value(metric_reader, _SKIP_COUNTER) == 3, (
        "one second past the cutoff the full skip branch fires: three "
        "dropped slots - the cliff is binary"
    )


# ── the mutation pin ───────────────────────────────────────────────────


async def test_mutation_pin_re_queueing_dropped_history_stays_red(
    metric_reader: InMemoryMetricReader,
) -> None:
    """THE runaway mutation pin.

    The hazard the skip design exists to prevent is not the drop itself,
    it is the well-meaning 'fix': someone makes the tick replay the
    dropped history — enqueue a job per missed occurrence (scheduled_at
    in the past) so 'nothing is lost' — and the */5 runaway fan-out is
    back, compounding under load, with every past occurrence re-entering
    the queue. This pin holds the contract that change violates, on
    every surface it would have to touch:

    * exactly ONE job is enqueued per fire attempt (no per-missed-slot
      fan-out),
    * no enqueued job is backdated into the dropped history (its
      ``scheduled_at`` is never before the catch-up cutoff),
    * the success UPDATE advances ``next_fire_at`` STRICTLY past the
      planning clock (the dropped slots are never re-queued for replay),
    * the census reads exactly 3 — a replay 'fix' that stopped skipping
      would report 0; one that both skipped and replayed would add jobs.

    A change that re-queues dropped history MUST turn this red.
    """
    schedule_id = new_uuid()
    conn = _FakeCronConn(
        schedule_rows=[_overdue_row(actor="runaway_actor", schedule_id=schedule_id)],
        actor_config_rows=[_make_actor_config_row(actor="runaway_actor")],
    )
    settings = _runaway_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fired = await _tick(conn, settings, backend)

    assert fired == 1
    jobs = await backend.list_jobs(JobFilter(actor="runaway_actor"))
    assert len(jobs) == 1, "one fire attempt, one job - never one per dropped slot"
    cutoff = _NOW - timedelta(seconds=300)
    for job in jobs:
        assert job.scheduled_at >= cutoff, (
            f"job {job.id} is backdated into the dropped history "
            f"({job.scheduled_at} < cutoff {cutoff}): dropped slots were "
            "re-queued for replay - the runaway fan-out hazard"
        )

    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    assert len(success_updates[0][1][0]) == 1, "exactly one schedule advanced, once"
    assert _first_success_next_fire(conn) > _NOW, (
        "next_fire_at must land strictly in the future - the recompute "
        "is what keeps history dropped BY DESIGN"
    )

    assert counter_value(metric_reader, _SKIP_COUNTER) == 3, (
        "the census says exactly 3 - a replay 'fix' that stopped "
        "skipping would report 0, one that also fanned out would have "
        "added jobs above"
    )
    assert _gauge_depths(metric_reader) == {"runaway_actor": 3}
