"""Fixed-quota keyed buckets: which backends idle eviction must hold.

#244: ``TokenBucket.holds_consumed_quota`` held EVERY Postgres fixed-quota
keyed bucket — spent or not — so once a worker had seen
``max_keyed_rate_limits`` distinct keys, every NEW key raised
``ReservationUnavailable`` (routed by the consumer to the 429
snooze/reschedule semantics — new tenants livelocked until process
restart). The hold's premise was that evicting the registry entry would
reset a spent quota; for the PG backend that premise is false twice over:

* no row-delete path can lose the quota — the per-worker pending-reclaim
  drain and the maintenance leader's fleet sweep share
  ``_no_consumed_quota_sql``, which vetoes deleting a row whose fixed
  quota is partly spent;
* re-materialization resumes from the surviving row — the acquire path
  preseeds ``ON CONFLICT DO NOTHING`` and reads the existing state under
  the bucket row's lock.

The memory backend is the one real hold: its token state lives only on
the in-process instance, so eviction would genuinely reset a spent quota.

This file pins the predicate per backend, the eviction's use of it, the
cap-refusal regression (the #244 reproduction, inverted), and — against
real Postgres — that evict-then-reacquire never resets or double-grants a
spent fixed quota (the red-team shape: two workers evict and
re-materialize the same key concurrently).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.exceptions import ReservationUnavailable
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedRateLimitRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _TenantPayload(BaseModel):
    tenant_id: str


def _pg_ref(base_name: str, *, capacity: float = 5.0) -> KeyedRateLimitRef:
    """A PG-backend FIXED-QUOTA keyed ref — the #244 shape."""
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=capacity,
        refill_per_second=0.0,
        backend="postgres",
    )


def _mem_ref(base_name: str, *, capacity: float = 5.0) -> KeyedRateLimitRef:
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=capacity,
        refill_per_second=0.0,
        backend="memory",
    )


def _settings(**overrides: Any) -> WorkerSettings:
    base: dict[str, Any] = {
        "TASKQ_PG_DSN": "postgresql://u:p@h:5432/db",
        "TASKQ_SCHEMA_NAME": f"tfix_{new_base62()}".lower(),
    }
    base.update(overrides)
    return WorkerSettings.load_from_dict(base, validate=False)


def _seed_idle(reg: RateLimitRegistry, *buckets: str) -> None:
    """Stamp every bucket's tracking entry far past the idle threshold."""
    for bucket in buckets:
        reg._keyed_rate_limit_last_used[bucket] = monotonic() - 7200.0  # pyright: ignore[reportPrivateUsage]  # Why: seeding entries idle for eviction, the pattern test_keyed_rate_limit_bucket_reclamation.py established.


# ── Unit: the predicate per backend ──────────────────────────────────────


async def test_pg_fixed_quota_bucket_is_not_held_spent_or_not() -> None:
    """A PG fixed-quota bucket never holds its registry entry on eviction:
    the row-delete vetoes own the state-safety guarantee, so the in-process
    hold is redundant bookkeeping — and (pre-fix) it was the cap-filling
    leak that refused every new key past ``max_keyed_rate_limits`` (#244)."""
    for spent in (False, True):
        # The instance carries no token state on the postgres backend —
        # the pre-fix hold could not even see spentness, it keyed purely
        # off (refill == 0, backend == postgres).
        tb = TokenBucket(name=f"tbfq_{spent}", capacity=5, refill_per_second=0, backend="postgres")
        assert tb.holds_consumed_quota() is False, (
            f"backend=postgres fixed-quota bucket (spent={spent}) must not "
            "hold idle eviction — the quota lives in the rate_limit_buckets "
            "row, and both delete paths veto deleting a spent one"
        )


async def test_memory_fixed_quota_bucket_held_only_when_actually_spent() -> None:
    """The memory backend IS a real hold: token state lives on the
    instance, so eviction would reset a spent quota. A full (unspent)
    bucket is evictable — resetting a full bucket to full loses nothing."""
    clock = FakeClock(_START)
    spent = TokenBucket(name="tbfq_mem_spent", capacity=5, refill_per_second=0, backend="memory")
    full = TokenBucket(name="tbfq_mem_full", capacity=5, refill_per_second=0, backend="memory")

    for _ in range(2):
        decision = await spent.acquire(1.0, clock=clock)
        assert decision.allowed

    assert spent.holds_consumed_quota() is True, (
        "a memory fixed-quota bucket that spent quota must be held — "
        "eviction would silently reset the drained quota to full"
    )
    assert full.holds_consumed_quota() is False, (
        "a memory fixed-quota bucket at full capacity must be evictable — "
        "holding it fills the keyed cap with never-used entries for nothing"
    )


async def test_refilling_buckets_are_never_held() -> None:
    """Refilling buckets converge back toward full on their own, so
    eviction forfeits at most one refill window — no backend holds them."""
    for backend in ("postgres", "memory", "redis"):
        tb = TokenBucket(name=f"tbfq_r_{backend}", capacity=5, refill_per_second=1.0, backend=backend)  # pyright: ignore[arg-type]  # Why: literal backend names, the constructor's own domain.
        assert tb.holds_consumed_quota() is False


# ── Unit: the eviction and the cap-refusal regression ────────────────────


async def test_idle_eviction_recycles_pg_fixed_quota_entries() -> None:
    """The eviction sweep (and the opportunistic cap-pressure path that
    calls it) must recycle idle PG fixed-quota keyed entries — the #244
    reproduction: cap 2, two idle never-again-used PG fixed-quota keys,
    and the third key must resolve instead of being refused forever."""
    reg = RateLimitRegistry()
    settings = _settings(TASKQ_MAX_KEYED_RATE_LIMITS="2")

    first = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing keyed buckets without acquiring, the pattern test_keyed_rate_limit_bucket_reclamation.py established.
        _pg_ref("tbfq-cap"), _TenantPayload(tenant_id="k1"), settings=settings
    )
    second = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
        _pg_ref("tbfq-cap"), _TenantPayload(tenant_id="k2"), settings=settings
    )
    _seed_idle(reg, first, second)

    # The pre-fix behavior this test replaces: the third resolution raised
    # ReservationUnavailable because both held entries counted against the
    # cap forever.
    third = await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]  # Why: as above.
        _pg_ref("tbfq-cap"), _TenantPayload(tenant_id="k3"), settings=settings
    )
    assert third == "tbfq-cap:k3"
    assert reg.has_rate_limit(third), "the refused-forever key must now materialize"

    # The two idle entries were recycled by the opportunistic eviction the
    # cap-hit triggered (a fresh registry's first scan always runs).
    assert not reg.has_rate_limit(first)
    assert not reg.has_rate_limit(second)


async def test_idle_eviction_still_holds_spent_memory_fixed_quota() -> None:
    """The memory exemption survives the fix: a partially spent memory
    fixed-quota bucket is preserved by the same sweep that now recycles
    PG entries — eviction there WOULD reset the spent quota."""
    reg = RateLimitRegistry()
    clock = FakeClock(_START)
    ref = _mem_ref("tbfq-mem-hold")

    acquired = await reg.acquire_for_actor(
        rate_limits=[ref],
        reservations=[],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_TenantPayload(tenant_id="mem"),
        clock=clock,
        settings=_settings(),
    )
    assert len(acquired) == 1
    bucket = "tbfq-mem-hold:mem"
    _seed_idle(reg, bucket)

    assert reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0)) == 0, (
        "a spent memory fixed-quota bucket must stay held — its quota "
        "lives only on the instance and eviction would reset it"
    )
    assert reg.has_rate_limit(bucket)


# ── PG integration: evict-then-reacquire state safety ────────────────────


class TestPgFixedQuotaEvictReacquire:
    """Evicting a PG fixed-quota keyed entry must neither reset its spent
    quota (the premise the pre-fix hold got wrong) nor double-grant under
    concurrent re-materialization (the red-team shape: two workers evict
    and re-materialize the same key at once)."""

    pytestmark = pytest.mark.integration

    async def _fresh_schema(self, pg_dsn: str) -> str:
        schema = f"tfix_{new_base62()}".lower()
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            await apply_pending(conn, schema=schema)
        finally:
            await conn.close()
        return schema

    async def _drop_schema(self, pg_dsn: str, schema: str) -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()

    async def test_spent_quota_survives_eviction_and_rematerialization(
        self, pg_dsn: str
    ) -> None:
        """The #244 verification pass on real PG, now pinned: evicting a
        spent PG fixed-quota entry keeps its row (the drain's delete veto)
        and re-resolving the key resumes from the surviving state — the
        budget the tenant already spent is never handed back."""
        schema = await self._fresh_schema(pg_dsn)
        settings = _settings(TASKQ_PG_DSN=pg_dsn, TASKQ_SCHEMA_NAME=schema)
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
        try:
            reg = RateLimitRegistry()
            ref = _pg_ref("tfix-resume", capacity=5.0)

            # Spend 2 of 5 tokens.
            for _ in range(2):
                acquired = await reg.acquire_for_actor(
                    rate_limits=[ref],
                    reservations=[],
                    job_id=new_uuid(),
                    worker_id=new_uuid(),
                    payload=_TenantPayload(tenant_id="acme"),
                    pg_pool=pool,
                    settings=settings,
                )
                assert len(acquired) == 1

            bucket = "tfix-resume:acme"
            state = await pool.fetchval(
                f'SELECT state->>\'tokens\' FROM "{schema}".rate_limit_buckets '  # noqa: S608  # Why: per-test schema identifier, not user input; bucket_name is $1-bound below.
                "WHERE bucket_name = $1",
                bucket,
            )
            assert float(state) == 3.0, "fixture broken: two of five tokens must be spent"

            # Evict the registry entry and run the row reclamation the
            # sweep performs — the drain's DELETE must VETO on the spent
            # row (the consumed-quota guard), so the state survives.
            _seed_idle(reg, bucket)
            assert reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0)) == 1
            assert not reg.has_rate_limit(bucket)
            await reg.drain_pending_reservation_reclaims(pool)

            surviving = await pool.fetchval(
                f'SELECT state->>\'tokens\' FROM "{schema}".rate_limit_buckets '  # noqa: S608  # Why: as above.
                "WHERE bucket_name = $1",
                bucket,
            )
            assert surviving is not None and float(surviving) == 3.0, (
                "the reclaim drain deleted a partly-spent fixed-quota row — "
                "the consumed-quota veto (_no_consumed_quota_sql) must keep it"
            )

            # Re-materialize the same key: the acquire path preseeds ON
            # CONFLICT DO NOTHING and resumes from the surviving row.
            resumed = await reg.acquire_for_actor(
                rate_limits=[ref],
                reservations=[],
                job_id=new_uuid(),
                worker_id=new_uuid(),
                payload=_TenantPayload(tenant_id="acme"),
                pg_pool=pool,
                settings=settings,
            )
            assert len(resumed) == 1
            decision = resumed[0].decision
            assert decision.allowed
            assert decision.remaining == 2.0, (
                f"re-materialization must resume the spent quota (expected "
                f"2.0 remaining after resuming from 3.0 and spending one, got "
                f"{decision.remaining}) — a reset to full capacity hands "
                "back a budget the tenant already spent"
            )

            # Only the remaining 2 tokens are grantable; the 6th total
            # acquire is denied — the registry routes the decision's
            # denial into ReservationUnavailable (the 429 channel the
            # consumer snoozes on), with the fixed-quota None retry hint
            # substituted by the flat backoff constant.
            for _ in range(2):
                more = await reg.acquire_for_actor(
                    rate_limits=[ref],
                    reservations=[],
                    job_id=new_uuid(),
                    worker_id=new_uuid(),
                    payload=_TenantPayload(tenant_id="acme"),
                    pg_pool=pool,
                    settings=settings,
                )
                assert len(more) == 1
                assert more[0].decision.allowed
            with pytest.raises(ReservationUnavailable):
                await reg.acquire_for_actor(
                    rate_limits=[ref],
                    reservations=[],
                    job_id=new_uuid(),
                    worker_id=new_uuid(),
                    payload=_TenantPayload(tenant_id="acme"),
                    pg_pool=pool,
                    settings=settings,
                )
            final_tokens = await pool.fetchval(
                f'SELECT state->>\'tokens\' FROM "{schema}".rate_limit_buckets '  # noqa: S608  # Why: per-test schema identifier, not user input; bucket_name is $1-bound below.
                "WHERE bucket_name = $1",
                bucket,
            )
            assert float(final_tokens) == 0.0, (
                "the whole fixed quota was spent across the eviction cycle — "
                "the budget never reset and never over-granted"
            )
        finally:
            await pool.close()
            await self._drop_schema(pg_dsn, schema)

    async def test_concurrent_rematerialization_never_double_grants(
        self, pg_dsn: str
    ) -> None:
        """Two workers (registries) evict their entries for the same
        fixed-quota key and then re-materialize concurrently: the row
        preseed (ON CONFLICT DO NOTHING) plus the locked state read must
        serialize the spend — total grants across both workers never
        exceed the capacity."""
        schema = await self._fresh_schema(pg_dsn)
        settings = _settings(TASKQ_PG_DSN=pg_dsn, TASKQ_SCHEMA_NAME=schema)
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=2, max_size=4)
        capacity = 6
        try:
            reg_a = RateLimitRegistry()
            reg_b = RateLimitRegistry()
            ref = _pg_ref("tfix-race", capacity=float(capacity))
            payload = _TenantPayload(tenant_id="acme")
            bucket = "tfix-race:acme"

            # Both workers have seen the key; both evict their entries.
            for reg in (reg_a, reg_b):
                await reg._resolve_rate_limit_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing the key on both registries before evicting, as two workers would each have done.
                    ref, payload, settings=settings, pg_pool=pool
                )
                _seed_idle(reg, bucket)
                assert reg.evict_idle_keyed_rate_limits(idle_for=timedelta(0)) == 1

            # N concurrent re-materializing acquires across both workers.
            async def _one(reg: RateLimitRegistry, job_id: UUID) -> bool:
                try:
                    acquired = await reg.acquire_for_actor(
                        rate_limits=[ref],
                        reservations=[],
                        job_id=job_id,
                        worker_id=new_uuid(),
                        payload=payload,
                        pg_pool=pool,
                        settings=settings,
                    )
                except ReservationUnavailable:
                    return False
                assert len(acquired) == 1
                return acquired[0].decision.allowed

            jobs = [(reg_a if i % 2 == 0 else reg_b, new_uuid()) for i in range(2 * capacity)]
            results = await asyncio.gather(*[_one(reg, j) for reg, j in jobs])
            granted = sum(1 for allowed in results if allowed)

            assert granted <= capacity, (
                f"{granted} grants for a capacity-{capacity} fixed quota — "
                "concurrent evict+re-materialization double-granted quota "
                "(the preseed/locked-read serialization is broken)"
            )
            assert granted == capacity, (
                f"only {granted} of {capacity} tokens were grantable — the "
                "fused cold-start path under-granted a full fixed quota"
            )
        finally:
            await pool.close()
            await self._drop_schema(pg_dsn, schema)
