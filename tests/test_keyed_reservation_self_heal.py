# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""Pins for the keyed-reservation acquire-path heal and pending-reclaim bookkeeping.

The reclamation drain (a sibling worker deleting an evicted keyed
bucket's idle ``reservation_slots`` rows) creates a cross-worker trap:
a bucket that stays REGISTERED on this worker while its rows are
deleted elsewhere denies every acquisition forever - the registry's
reuse path never re-runs ``ensure_slots`` for an already-registered
name, so the registered-bucket-with-zero-rows condition is permanent
until restart. The heal pinned here closes it: on denial, a
keyed-materialized bucket gets one gated existence probe; zero rows
re-materialises and retries once.

The boundary is as critical as the heal itself: a genuinely BUSY
bucket (rows present, all held) must pay no ``ensure_slots`` write and
no retry - ordinary contention is not a heal condition - and the
probe's cost is bounded by the heal window to at most one probe per
bucket per window per worker. The pending-reclaim set that feeds the
drain must itself stay bounded: eviction refuses to record past its
cap, leaving the entry registered for a later sweep.
"""

import asyncio
from datetime import timedelta
from time import monotonic
from typing import Any

import asyncpg
import pytest
import structlog
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pydantic import BaseModel

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq._ids import new_uuid
from taskq.exceptions import ReservationUnavailable
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.settings import WorkerSettings
from taskq.testing.otel import counter_value, histogram_points

_SESSION_ID = "s1"


class _SessionPayload(BaseModel):
    session_id: str


def _schema() -> str:
    """This file's dedicated schema (local, per the suite-hygiene rule
    against module-level schema constants)."""
    return "taskq_keyed_selfheal_test"


def _settings(pg_dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": _schema()},
        validate=False,
    )


async def _fresh_schema(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_schema()}" CASCADE')
        await apply_pending(conn, schema=_schema())
    finally:
        await conn.close()


async def _slot_rows(pool: asyncpg.Pool, bucket: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


async def _delete_rows(pool: asyncpg.Pool, bucket: str) -> None:
    """A sibling worker's reclaim drain, distilled: delete the bucket's rows."""
    async with pool.acquire() as conn:
        await conn.execute(
            f'DELETE FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


def _ref(base_name: str, *, slots: int = 2) -> KeyedReservationRef:
    return KeyedReservationRef.typed(
        _SessionPayload,
        base_name=base_name,
        key_fn=lambda p: p.session_id,
        slots=slots,
        lease=timedelta(minutes=10),
    )


async def _acquire(
    reg: RateLimitRegistry,
    ref: KeyedReservationRef,
    pool: asyncpg.Pool,
    settings: WorkerSettings,
):
    return await reg.acquire_for_actor(
        rate_limits=[],
        reservations=[ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_SessionPayload(session_id=_SESSION_ID),
        pg_pool=pool,
        settings=settings,
    )


@pytest.mark.integration
async def test_acquire_heals_keyed_bucket_whose_rows_were_deleted(pg_dsn: str) -> None:
    """A registered keyed bucket with zero rows must heal on the next
    acquire - the registered-bucket-with-zero-rows trap is permanent
    without the heal, because the reuse path never re-runs ensure_slots."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = _ref("heal-probe")
        settings = _settings(pg_dsn)

        acquired = await _acquire(reg, ref, pool, settings)
        await reg.release_for_actor(acquired)
        bucket = "heal-probe:s1"
        assert await _slot_rows(pool, bucket) == 2, "fixture broken: no rows to delete"

        await _delete_rows(pool, bucket)
        assert await _slot_rows(pool, bucket) == 0, "fixture broken: rows survived the delete"

        with structlog.testing.capture_logs() as captured:
            reacquired = await _acquire(reg, ref, pool, settings)

        assert len(reacquired) == 1
        assert reacquired[0].name == bucket
        assert await _slot_rows(pool, bucket) == 2, "the heal must re-materialise the rows"
        assert any(e.get("event") == "keyed-reservation-healed" for e in captured), (
            "the acquire succeeded without a heal event - the rows came back by "
            "some other path, and this test no longer pins the heal"
        )
    finally:
        await pool.close()


@pytest.mark.integration
async def test_busy_denial_issues_no_heal(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All slots genuinely held by another job: the denial is ordinary
    contention - no ensure_slots, no row mutation, no retry."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = _ref("busy-probe", slots=1)
        settings = _settings(pg_dsn)

        # Job A holds the only slot for the rest of this test.
        await _acquire(reg, ref, pool, settings)
        bucket = "busy-probe:s1"
        rows_before = await _slot_rows(pool, bucket)
        assert rows_before == 1, "fixture broken: the only slot is not materialised"

        ensure_calls = 0
        real_ensure = ConcurrencyReservation.ensure_slots

        async def _spy_ensure(
            self: ConcurrencyReservation, pool: "asyncpg.Pool", **_kwargs: object
        ) -> None:
            nonlocal ensure_calls
            ensure_calls += 1
            await real_ensure(self, pool)

        monkeypatch.setattr(ConcurrencyReservation, "ensure_slots", _spy_ensure)

        with pytest.raises(ReservationUnavailable):
            await _acquire(reg, ref, pool, settings)

        assert ensure_calls == 0, "a busy denial must not re-materialise the bucket"
        assert await _slot_rows(pool, bucket) == rows_before
    finally:
        await pool.close()


@pytest.mark.integration
async def test_heal_window_does_not_survive_eviction_and_re_registration(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The heal window bounds a busy bucket to one probe per window - and
    must not outlive the bucket: after evict + re-register + a fresh row
    deletion, the FIRST denied acquire heals again."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = _ref("window-probe", slots=1)
        settings = _settings(pg_dsn)
        bucket = "window-probe:s1"

        # Job A holds the only slot; the bucket stays registered and busy.
        acquired_a = await _acquire(reg, ref, pool, settings)

        probes = 0
        real_probe = ConcurrencyReservation.slot_rows_exist

        async def _spy_probe(
            self: ConcurrencyReservation, pool: "asyncpg.Pool", **_kwargs: object
        ) -> bool:
            nonlocal probes
            probes += 1
            return await real_probe(self, pool)

        monkeypatch.setattr(ConcurrencyReservation, "slot_rows_exist", _spy_probe)

        # First denial: the window is empty, the probe runs, rows are
        # present - ordinary contention, denied.
        with pytest.raises(ReservationUnavailable):
            await _acquire(reg, ref, pool, settings)
        assert probes == 1, "fixture broken: the busy denial did not probe"

        # Second denial inside the window: the gate suppresses the probe.
        with pytest.raises(ReservationUnavailable):
            await _acquire(reg, ref, pool, settings)
        assert probes == 1, "a busy bucket must pay at most one probe per heal window"

        # Evict (seeding the entry idle), re-register, delete the rows:
        # the first denied acquire must heal again - the window from the
        # previous incarnation must not suppress it.
        await reg.release_for_actor(acquired_a)
        reg._keyed_reservation_last_used[bucket] = monotonic() - 7200.0  # pyright: ignore[reportPrivateUsage]  # Why: seeding the entry idle for eviction, matching test_leader_sweep_rl_registry.py's pattern.
        assert reg.evict_idle_keyed_reservations(idle_for=timedelta(0)) == 1

        acquired_b = await _acquire(reg, ref, pool, settings)
        await reg.release_for_actor(acquired_b)
        await _delete_rows(pool, bucket)

        probes_before_heal = probes
        reacquired = await _acquire(reg, ref, pool, settings)
        assert len(reacquired) == 1, (
            "the first denied acquire after evict + re-register must heal - "
            "the heal window survived the registration lifecycle"
        )
        assert probes == probes_before_heal + 1
    finally:
        await pool.close()


@pytest.mark.integration
async def test_busy_denial_window_defers_a_later_zero_rows_heal(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A busy denial's window stamp defers a zero-rows heal that lands
    inside the same window - the bounded trade-off for probe-per-denial
    protection - and the first denial after the window heals.

    Sequence: a contended bucket denies (probe, stamp); the rows then
    vanish (a sibling worker's drain) while the window still stands; the
    next denial is deferred with NO probe. Shrink the window to zero and
    the next denial probes, heals, and acquires.
    """
    # import_module, not `import ... as`: the ratelimit package re-exports
    # the `registry` SINGLETON under the same name as this submodule, so
    # `import taskq.ratelimit.registry as x` binds the instance, and a
    # monkeypatch against it never reaches the module global the heal
    # reads. Same workaround as test_keyed_reservation_hardening.py.
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = _ref("defer-probe", slots=1)
        settings = _settings(pg_dsn)
        bucket = "defer-probe:s1"

        # Job A holds the only slot; job B's denial probes once (busy) and
        # sets the window stamp.
        acquired_a = await _acquire(reg, ref, pool, settings)
        with pytest.raises(ReservationUnavailable):
            await _acquire(reg, ref, pool, settings)

        # The rows vanish while the window stands.
        await reg.release_for_actor(acquired_a)
        await _delete_rows(pool, bucket)

        monkeypatch.setattr(registry_mod, "_KEYED_RECLAIM_HEAL_WINDOW", timedelta(hours=1))
        with pytest.raises(ReservationUnavailable):
            await _acquire(reg, ref, pool, settings)

        # Past the window, the next denial heals and acquires.
        monkeypatch.setattr(registry_mod, "_KEYED_RECLAIM_HEAL_WINDOW", timedelta(0))
        reacquired = await _acquire(reg, ref, pool, settings)
        assert len(reacquired) == 1
        assert await _slot_rows(pool, bucket) == 1
    finally:
        await pool.close()


async def test_pending_reclaim_set_is_capped() -> None:
    """Eviction refuses to record past max_pending_reclaims: the set never
    exceeds the cap, and the overflow entries stay registered (re-scanned
    next sweep) with the depth visible on the pending gauge."""
    reg = RateLimitRegistry()
    ref = _ref("cap-probe", slots=1)
    for i in range(7):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing keyed entries without PG, matching test_keyed_reservation_hardening.py's pattern.
            ref, payload=_SessionPayload(session_id=f"k{i}"), pg_pool=None, settings=None
        )

    evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(0), max_pending_reclaims=5)

    assert evicted == 5, "the pending cap must veto the excess evictions"
    assert reg.has_pending_reservation_reclaims
    assert len(reg.reservations) == 2, "vetoed entries must stay registered for the next sweep"
    assert len(reg._keyed_reservation_last_used) == 2  # pyright: ignore[reportPrivateUsage]  # Why: the veto is observable as tracked entries the eviction left behind.
    assert otel_mod._keyed_reclaim_pending == 5  # pyright: ignore[reportPrivateUsage]  # Why: the pending-depth gauge's backing value is the observable signal the cap's skip must reach.


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation (mirrors test_silent_failure_guards.py)."""
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(
        obs_mod.INSTRUMENTATION_NAME,
        otel_mod._version(),  # pyright: ignore[reportPrivateUsage]  # Why: mirrors the otel_reader fixture in test_silent_failure_guards.py.
    )
    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: new_meter)
    otel_mod.set_otel_enabled(True)
    return reader


class _FailingPool:
    """A pool whose ``acquire`` raises - the drain's failure path."""

    def acquire(self, *, timeout: float | None = None) -> Any:
        raise asyncpg.PostgresConnectionError("pool acquire failed")


async def test_drain_failure_is_counted_and_pending_intact(
    otel_reader: InMemoryMetricReader,
) -> None:
    """A failing drain must never look like an empty one: the failure
    counter increments, a duration sample lands (the finally, not the
    success line), and the pending set survives for the next tick."""
    reg = RateLimitRegistry()
    ref = _ref("drain-fail-probe", slots=1)
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing a keyed entry without PG, matching test_keyed_reservation_hardening.py's pattern.
        ref, payload=_SessionPayload(session_id="k1"), pg_pool=None, settings=None
    )
    assert reg.evict_idle_keyed_reservations(idle_for=timedelta(0)) == 1
    assert reg.has_pending_reservation_reclaims

    with pytest.raises(asyncpg.PostgresConnectionError):
        await reg.drain_pending_reservation_reclaims(_FailingPool())  # type: ignore[arg-type]  # Why: a minimal stand-in for asyncpg.Pool; only acquire() is reached.

    assert counter_value(otel_reader, "taskq.ratelimit.reclaim_drain_failures") == 1
    assert histogram_points(otel_reader, "taskq.ratelimit.reclaim_drain_duration"), (
        "a failed drain must still leave a duration sample (the finally "
        "discipline - success-path-only instrumentation is the original "
        "invisibility)"
    )
    assert reg.has_pending_reservation_reclaims, "a failed drain must keep its backlog"


async def test_pending_cap_follows_the_setting_on_the_opportunistic_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The opportunistic eviction records pending reclaims under the
    SETTING-derived cap: with ``max_keyed_reservations=3``, a second
    eviction wave finds the pending set at the ceiling, every eviction in
    the wave is vetoed, and the new key is denied (fail-closed) until the
    drain empties the pending set. The constant fallback would let pending
    grow past the setting's ceiling while the tracked entries themselves
    are capped at it."""
    from importlib import import_module

    registry_mod = import_module("taskq.ratelimit.registry")

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_MAX_KEYED_RESERVATIONS": "3",
        },
        validate=False,
    )
    reg = RateLimitRegistry()
    ref = _ref("wave-probe", slots=1)

    fake_time = 1000.0
    monkeypatch.setattr(registry_mod, "monotonic", lambda: fake_time)
    for key in ("k1", "k2", "k3"):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing keyed entries without PG, matching the pattern above.
            ref, payload=_SessionPayload(session_id=key), pg_pool=None, settings=settings
        )
    assert len(reg._keyed_reservation_last_used) == 3  # pyright: ignore[reportPrivateUsage]  # Why: the cap is observable as the tracked-entry count.

    # Wave 1: all three idle; a new key hits the cap, the opportunistic
    # eviction records all three (pending at the setting's ceiling) and
    # the new key is admitted.
    fake_time = 5000.0
    await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
        ref, payload=_SessionPayload(session_id="k4"), pg_pool=None, settings=settings
    )
    assert otel_mod._keyed_reclaim_pending == 3  # pyright: ignore[reportPrivateUsage]  # Why: the pending-depth gauge's backing value is the observable signal.

    # Refill to the cap; all idle again.
    for key in ("k5", "k6"):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_SessionPayload(session_id=key), pg_pool=None, settings=settings
        )
    assert len(reg._keyed_reservation_last_used) == 3  # pyright: ignore[reportPrivateUsage]
    fake_time = 9000.0

    # Wave 2: pending is already at the setting-derived cap, so every
    # eviction in the wave is vetoed - pending never exceeds the setting
    # and the new key is denied until the drain empties pending.
    with pytest.raises(ReservationUnavailable):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]
            ref, payload=_SessionPayload(session_id="k7"), pg_pool=None, settings=settings
        )
    assert otel_mod._keyed_reclaim_pending == 3, (  # pyright: ignore[reportPrivateUsage]  # Why: the pending-depth gauge's backing value is the observable signal.
        "the opportunistic eviction must record under the settings-derived "
        "cap, not the constant fallback - pending may never exceed the setting"
    )
    assert len(reg._keyed_reservation_last_used) == 3, (  # pyright: ignore[reportPrivateUsage]  # Why: the veto is observable as tracked entries the eviction left behind.
        "vetoed entries must stay registered for the next sweep"
    )
    assert "wave-probe:k4" in reg.reservations
    assert "wave-probe:k6" in reg.reservations


async def test_cap_veto_is_logged_once_per_eviction_call() -> None:
    """At the pending cap, an eviction call that must shed N+1 buckets
    records N and logs the veto count ONCE: pending at cap means
    reclamation is falling behind (the drain cannot keep up with the
    eviction rate) - an onset signal for the operator alongside the
    steady pending-depth gauge, not a silent skip."""
    reg = RateLimitRegistry()
    ref = _ref("veto-probe", slots=1)
    for i in range(7):
        await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing keyed entries without PG, matching the pattern above.
            ref, payload=_SessionPayload(session_id=f"k{i}"), pg_pool=None, settings=None
        )

    with structlog.testing.capture_logs() as captured:
        evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(0), max_pending_reclaims=5)

    assert evicted == 5
    veto_logs = [e for e in captured if e.get("event") == "registry-keyed-reclaim-pending-cap-veto"]
    assert len(veto_logs) == 1, (
        "one aggregated veto line per eviction call - a line per shed entry "
        "re-creates the flood the aggregation exists to prevent"
    )
    assert veto_logs[0]["vetoed"] == 2
    assert veto_logs[0]["cap"] == 5
    assert len(reg._keyed_reservation_last_used) == 2, (  # pyright: ignore[reportPrivateUsage]  # Why: the veto is observable as tracked entries the eviction left behind.
        "shed entries must stay registered for the next sweep"
    )


@pytest.mark.integration
async def test_cancelled_heal_does_not_leave_a_standing_window_stamp(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A CancelledError propagating out of the heal probe must not leave
    the 60-second heal-window stamp set: the next denial probes
    immediately, instead of being suppressed for the rest of the window
    by a stamp from a heal that never ran to completion."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = _ref("cancel-probe", slots=1)
        settings = _settings(pg_dsn)

        # Job A holds the only slot; the bucket is busy for the whole test.
        await _acquire(reg, ref, pool, settings)

        probes = 0
        real_probe = ConcurrencyReservation.slot_rows_exist
        cancelled_once = False

        async def _cancel_once_probe(
            self: ConcurrencyReservation, pool: "asyncpg.Pool", **_kwargs: object
        ) -> bool:
            nonlocal probes, cancelled_once
            probes += 1
            if not cancelled_once:
                cancelled_once = True
                raise asyncio.CancelledError()
            return await real_probe(self, pool)

        monkeypatch.setattr(ConcurrencyReservation, "slot_rows_exist", _cancel_once_probe)

        # Job B's denial starts a heal; the probe is cancelled mid-flight
        # and the cancellation propagates - it is a task teardown, not a
        # heal outcome, so the composition rollback deliberately lets it
        # through untouched.
        with pytest.raises(asyncio.CancelledError):
            await _acquire(reg, ref, pool, settings)

        # Job C's denial must probe IMMEDIATELY: the cancelled heal left
        # no standing stamp, so the window does not suppress it.
        with pytest.raises(ReservationUnavailable):
            await _acquire(reg, ref, pool, settings)
        assert probes == 2, (
            "a cancelled heal must roll its window stamp back - the next "
            "denial probes immediately instead of waiting out the window"
        )
    finally:
        await pool.close()


@pytest.mark.integration
async def test_heal_failure_counts_every_attempt_but_warns_once_per_window(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    otel_reader: InMemoryMetricReader,
) -> None:
    """A persistently-failing probe retries the heal on EVERY denial (the
    stamp is rolled back so the next denial re-probes): the failure
    COUNTER records each attempt - metrics aggregate - but the WARNING is
    rate-limited to one per bucket per heal window. A busy bucket with a
    broken probe denies on every acquisition; a warning per failure would
    put one log line per denial into the log."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = _ref("failwarn-probe", slots=1)
        settings = _settings(pg_dsn)

        # Job A holds the only slot; the bucket is busy for the whole test.
        await _acquire(reg, ref, pool, settings)

        async def _failing_probe(
            self: ConcurrencyReservation, pool: "asyncpg.Pool", **_kwargs: object
        ) -> bool:
            raise ConnectionError("probe down")

        monkeypatch.setattr(ConcurrencyReservation, "slot_rows_exist", _failing_probe)

        with structlog.testing.capture_logs() as captured:
            with pytest.raises(ReservationUnavailable):
                await _acquire(reg, ref, pool, settings)
            with pytest.raises(ReservationUnavailable):
                await _acquire(reg, ref, pool, settings)

        assert counter_value(otel_reader, "taskq.ratelimit.reclaim_heal_failures") == 2, (
            "every failed heal attempt must count - the counter is the aggregate signal"
        )
        warn_events = [e for e in captured if e.get("event") == "keyed-reservation-heal-failed"]
        assert len(warn_events) == 1, (
            "the heal-failure warning must fire once per bucket per heal window - "
            "a busy bucket with a failing probe denies on every acquisition"
        )
    finally:
        await pool.close()
