"""Audit-coverage pins: the ratelimit boundary arms the suite never ran.

Every test here was written red-first against a specific uncovered line
(the `audit-cov` census, branch feat/audit-coverage): each documents the
arm it closes in its docstring, and each was mutation-proven (flipping
the target line makes the test fail).

Closed arms:
  - ``_sliding_window_redis._validate_script_reply``: the non-sequence,
    short-reply, impossible-verdict and out-of-range-count script-reply
    lies (the acquire path's trust boundary — only the peek paths had lie
    pins before).
  - ``_sliding_window_redis._peek_redis_log``: the negative-ZCARD lie.
  - ``_sliding_window_redis._peek_redis_gcra``: the non-finite TAT lie.
  - ``token_bucket._retry_after``: the non-finite and non-positive clamp
    arms; the refund script's client-swap rebinding arm.
  - ``_lock_budget.resolve_token_bucket_lock_timeout_ms``: the no-settings
    default arm.
  - ``_redis_utils.ensure_redis_script``: the double-checked lock's inner
    re-check arm (the second waiter's cache hit).
  - ``_redis_utils.with_pg_fallback``: the ``[redis]``-extra ImportError
    raise.
  - ``ratelimit.registry``: the reclaim-drain's schema guard, empty-schema
    skip and rate-limit twin, the keyed bucket publish-failed warning, and
    ``_upsert_rate_limit_bucket_row``'s schema guard.
  - ``backend._dispatch``: the queue-mode resolvers' schema guards and
    empty-queue short circuits (the public ``resolve_queue_modes``
    surface's own validation reach).
  - ``reservation``: the ``keyed`` lifecycle-origin property.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel

from taskq.exceptions import RateLimitStoreCorrupt
from taskq.ratelimit import SlidingWindow
from taskq.ratelimit._lock_budget import (
    resolve_sliding_window_lock_timeout_ms,
    resolve_token_bucket_lock_timeout_ms,
)
from taskq.ratelimit._redis_utils import ensure_redis_script, with_pg_fallback
from taskq.ratelimit._sliding_window_redis import (
    _acquire_redis_gcra,
    _acquire_redis_log,
    _peek_redis_gcra,
    _peek_redis_log,
)
from taskq.ratelimit.token_bucket import TokenBucket, _retry_after
from taskq.settings import WorkerSettings

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://u:p@h/d",
            "redis_url": "redis://lie-proxy:6379/0",
            "schema_name": "taskq_test",
        }
    )


class _LyingScript:
    """Duck-typed AsyncScript whose ``__call__`` returns a canned lie."""

    def __init__(self, reply: object) -> None:
        self._reply = reply

    async def __call__(self, keys: list[str], args: list[object]) -> object:
        return self._reply


class _LyingScriptClient:
    """Duck-typed redis client that registers lying scripts."""

    def __init__(self, reply: object) -> None:
        self._reply = reply

    def register_script(self, script: str) -> _LyingScript:
        return _LyingScript(self._reply)


# ── the acquire path's script-reply trust boundary ──────────────────────


async def test_log_acquire_non_sequence_reply_raises_the_sentinel() -> None:
    """A script reply that is not a list/tuple (a proxy collapsing the
    3-element contract to a bare int) raises the corrupt-store sentinel —
    the acquire boundary's first shape check."""
    sw = SlidingWindow(name="audit_lie_seq", limit=5, window=timedelta(seconds=10))
    with pytest.raises(RateLimitStoreCorrupt, match="3-element contract"):
        await _acquire_redis_log(
            sw,  # type: ignore[arg-type]
            new_uuid(),
            _LyingScriptClient(7),
            _settings(),
        )


async def test_log_acquire_truncated_reply_raises_the_sentinel() -> None:
    """A 2-element reply (a truncated EVALSHA answer) raises the sentinel."""
    sw = SlidingWindow(name="audit_lie_trunc", limit=5, window=timedelta(seconds=10))
    with pytest.raises(RateLimitStoreCorrupt, match="3-element contract"):
        await _acquire_redis_log(
            sw,  # type: ignore[arg-type]
            new_uuid(),
            _LyingScriptClient([1, 0]),
            _settings(),
        )


async def test_gcra_acquire_impossible_verdict_raises_the_sentinel() -> None:
    """A reply whose allowed verdict is neither 0 nor 1 (a proxy answering
    a count where a verdict belongs) raises the sentinel — the acquire
    boundary's verdict-domain check."""
    sw = SlidingWindow(
        name="audit_lie_verdict", limit=5, window=timedelta(seconds=10), style="gcra"
    )
    with pytest.raises(RateLimitStoreCorrupt, match="impossible verdict"):
        await _acquire_redis_gcra(
            sw,  # type: ignore[arg-type]
            _LyingScriptClient([7, 0, 0]),
            _settings(),
        )


async def test_log_acquire_out_of_range_count_raises_the_sentinel() -> None:
    """A count above the limit (a reply that skipped the script's own
    ZADD gate) is a lie; the sentinel, not a phantom allowance."""
    sw = SlidingWindow(name="audit_lie_count", limit=5, window=timedelta(seconds=10))
    with pytest.raises(RateLimitStoreCorrupt, match="outside"):
        await _acquire_redis_log(
            sw,  # type: ignore[arg-type]
            new_uuid(),
            _LyingScriptClient([1, 9, 0]),
            _settings(),
        )


# ── the peek paths' remaining lie shapes ────────────────────────────────


class _PeekRedis:
    """Duck-typed client for the peek paths: controllable TIME and hash."""

    def __init__(self, *, time_reply: object, zcount_reply: object) -> None:
        self._time = time_reply
        self._zcount = zcount_reply

    async def time(self) -> object:
        return self._time

    async def zcount(self, key: str, min: str, max: str) -> object:
        return self._zcount

    async def zrangebyscore(
        self, key: str, min: str, max: str, start: int = 0, num: int = 1, withscores: bool = False
    ) -> object:
        return []


async def test_log_peek_negative_zcount_raises_the_sentinel() -> None:
    """A negative ZCARD reply is a lie (a cardinality cannot be negative);
    the peek fails closed with the sentinel."""
    with pytest.raises(RateLimitStoreCorrupt, match="negative count"):
        await _peek_redis_log(
            SlidingWindow("audit_neg", limit=5, window=timedelta(seconds=10), style="log"),
            redis_client=_PeekRedis(time_reply=[1767225600, 0], zcount_reply=-5),
            settings=_settings(),
        )


class _GcraPeekRedis:
    """Duck-typed client for the GCRA peek: controllable TIME and TAT."""

    def __init__(self, *, time_reply: object, get_reply: object) -> None:
        self._time = time_reply
        self._get = get_reply

    async def time(self) -> object:
        return self._time

    async def get(self, key: str) -> object:
        return self._get


async def test_gcra_peek_non_finite_tat_raises_the_sentinel() -> None:
    """A NaN TAT read passes the type check (float) and must still fail
    closed: every window comparison downstream runs in that domain."""
    with pytest.raises(RateLimitStoreCorrupt, match="non-finite TAT"):
        await _peek_redis_gcra(
            SlidingWindow("audit_nan", limit=5, window=timedelta(seconds=10), style="gcra"),
            redis_client=_GcraPeekRedis(time_reply=[1767225600, 0], get_reply=float("nan")),
            settings=_settings(),
        )


# ── the primitives' clamp and default arms ──────────────────────────────


def test_retry_after_non_finite_collapses_to_max_ttl() -> None:
    """inf/NaN seconds (a lying denial hint) clamp to the max TTL instead
    of raising out of the primitive."""
    assert _retry_after(float("inf")) == _retry_after(10**9)
    assert _retry_after(float("nan")) == _retry_after(10**9)


def test_retry_after_non_positive_is_zero() -> None:
    """A non-positive hint means 'no wait': the zero timedelta, never a
    negative one."""
    assert _retry_after(0.0) == timedelta(0)
    assert _retry_after(-1.0) == timedelta(0)


async def test_refund_script_rebinds_after_a_client_swap() -> None:
    """A refund script cached on one redis client is NOT reused for the
    next client: the get() closure's client-identity check misses and the
    script re-registers against the live client — the swap-heal contract
    ensure_redis_script's cache-binding rule exists for."""
    tb = TokenBucket(name="audit_refund_swap", capacity=5.0, refill_per_second=1.0, backend="redis")
    first = await tb._ensure_refund_script(_LyingScriptClient("s1"))  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    second = await tb._ensure_refund_script(_LyingScriptClient("s2"))  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    assert first is not second, "a swapped client must re-register, not reuse the dead binding"
    again = await tb._ensure_refund_script(tb._redis_refund_script_client)  # pyright: ignore[reportPrivateUsage]
    assert again is second, "the same client's cached script is reused"


def test_lock_budget_defaults_without_settings() -> None:
    """No override and no settings resolves to the shipped default — the
    arm the operator-override path shadows."""
    assert resolve_token_bucket_lock_timeout_ms(None, None, 250.0) == 250.0
    assert resolve_sliding_window_lock_timeout_ms(None, None, 500.0) == 500.0


# ── the script cache's double-checked lock ──────────────────────────────


async def test_ensure_redis_script_inner_recheck_returns_cached() -> None:
    """The waiter that takes the lock AFTER the cache was filled returns
    the cached script through the lock's own re-check — the arm that keeps
    a client swap from registering the same script twice against the
    live client.

    The call counter shapes the schedule deterministically: the first
    caller's outer and inner checks miss (calls 1-2), the second caller's
    outer check misses too (call 3) while the first caller's register()
    has already filled the cache, so the second caller's INNER re-check
    (call 4) is the arm that returns.
    """
    registered: list[str] = []
    calls = {"n": 0}
    lock = asyncio.Lock()

    def register() -> str:
        registered.append("script")
        return "script-2"

    def get() -> str | None:
        calls["n"] += 1
        if calls["n"] <= 3:
            return None
        return "script-2"

    def set_(script: object) -> None:
        return None

    first, second = await asyncio.gather(
        ensure_redis_script(get, set_, register, lock),
        ensure_redis_script(get, set_, register, lock),
    )
    assert first == "script-2"
    assert second == "script-2"
    assert len(registered) == 1


# ── the [redis]-extra guard ─────────────────────────────────────────────


async def test_with_pg_fallback_without_redis_extra_raises_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reaching the redis wrapper with the extra uninstalled raises the
    operator-actionable ImportError, never a bare NameError from the
    missing module."""
    monkeypatch.setitem(__import__("sys").modules, "redis", None)
    monkeypatch.setitem(__import__("sys").modules, "redis.exceptions", None)
    with pytest.raises(ImportError, match=r"Install it with: pip install 'taskq\[redis\]'"):
        await with_pg_fallback(
            AsyncMock(),  # type: ignore[arg-type]
            AsyncMock(),  # type: ignore[arg-type]
            bucket_name="audit_import_error",
            settings=_settings(),
        )


# ── the reclaim drain's guards ──────────────────────────────────────────


def _registry() -> Any:
    import sys

    registry_mod = sys.modules["taskq.ratelimit.registry"]  # type: ignore[assignment]  # Why: as noted
    return registry_mod.RateLimitRegistry()


class _FakePoolConn:
    async def execute(self, *args: object, **kwargs: object) -> str:
        return "DELETE 0"

    async def fetch(self, *args: object, **kwargs: object) -> list[object]:
        return []


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakePoolConn()

    def acquire(self, timeout: float | None = None):
        conn = self.conn

        class _ACM:
            async def __aenter__(self) -> _FakePoolConn:
                return conn

            async def __aexit__(self, *exc: object) -> None:
                return None

        return _ACM()


async def test_drain_reclaims_invalid_schema_raises() -> None:
    """A pending schema that fails the identifier validation raises —
    the drain refuses to interpolate an unvalidated schema into SQL."""
    reg = _registry()
    reg._pending_reservation_reclaims["bad schema; drop table"] = {"b": None}
    with pytest.raises(ValueError, match="invalid schema identifier"):
        await reg.drain_pending_reservation_reclaims(_FakePool())


async def test_drain_reclaims_empty_schema_is_skipped() -> None:
    """A schema whose pending set drained concurrently is dropped and
    skipped, not retried forever."""
    reg = _registry()
    reg._pending_reservation_reclaims["taskq_test"] = {}
    deleted = await reg.drain_pending_reservation_reclaims(_FakePool())
    assert deleted == 0
    assert "taskq_test" not in reg._pending_reservation_reclaims


async def test_drain_rate_limit_reclaims_invalid_schema_and_empty_skip() -> None:
    """The rate-limit twin: same schema guard, same empty-schema skip."""
    reg = _registry()
    reg._pending_rate_limit_reclaims["bad schema; drop table"] = {"b": None}
    with pytest.raises(ValueError, match="invalid schema identifier"):
        await reg.drain_pending_reservation_reclaims(_FakePool())
    reg2 = _registry()
    reg2._pending_rate_limit_reclaims["taskq_test"] = {}
    deleted = await reg2.drain_pending_reservation_reclaims(_FakePool())
    assert deleted == 0
    assert "taskq_test" not in reg2._pending_rate_limit_reclaims


# ── the queue-mode resolvers' public-surface guards ─────────────────────


def test_resolve_queue_modes_by_queue_refuses_an_invalid_schema() -> None:
    """The shared query core re-validates the schema it interpolates: the
    public ``PostgresBackend.resolve_queue_modes`` static method accepts an
    arbitrary schema and caller-supplied connection, so construction-time
    validation on the backend instance cannot cover every reach."""
    from taskq.backend._dispatch import (
        _resolve_queue_modes_by_queue,  # pyright: ignore[reportPrivateUsage]
    )

    with pytest.raises(ValueError, match="invalid schema identifier"):
        asyncio.run(_resolve_queue_modes_by_queue(None, ["q"], "bad schema; drop table"))  # type: ignore[arg-type]


def test_resolve_queue_modes_refuses_an_invalid_schema() -> None:
    """Same invariant at the mode-set resolver's own seam — and the guard
    fires even when NO query would run (empty queues short-circuit after
    it, the interpolation-reach rule does not depend on the workload)."""
    from taskq.backend._dispatch import _resolve_queue_modes  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(ValueError, match="invalid schema identifier"):
        asyncio.run(_resolve_queue_modes(None, ["q"], "bad schema; drop table"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="invalid schema identifier"):
        asyncio.run(_resolve_queue_modes(None, [], "bad schema; drop table"))  # type: ignore[arg-type]


def test_resolve_queue_modes_empty_queues_short_circuits() -> None:
    """No queues named: the answer is the strict-FIFO singleton, no query
    spent."""
    from taskq.backend._dispatch import _resolve_queue_modes  # pyright: ignore[reportPrivateUsage]

    assert asyncio.run(_resolve_queue_modes(None, [], "taskq_test")) == {"strict_fifo"}  # type: ignore[arg-type]


def test_resolve_queue_modes_by_queue_empty_queues_short_circuits() -> None:
    """The by-queue resolver answers an empty mapping the same way."""
    from taskq.backend._dispatch import (
        _resolve_queue_modes_by_queue,  # pyright: ignore[reportPrivateUsage]
    )

    assert asyncio.run(_resolve_queue_modes_by_queue(None, [], "taskq_test")) == {}  # type: ignore[arg-type]


# ── the keyed bucket publish's failure arm ──────────────────────────────


class _TenantPayload(BaseModel):
    tenant_id: str


def _pg_ref(base_name: str) -> Any:
    from taskq.ratelimit.refs import KeyedRateLimitRef

    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=5.0,
        refill_per_second=0.0,
        backend="postgres",
    )


async def test_keyed_bucket_first_pool_bearing_publish_failure_warns_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A keyed bucket materialized WITHOUT a pool publishes on its first
    pool-bearing reuse; a failed publish warns and continues — the row is
    observability metadata, never an acquisition precondition."""
    import sys

    registry_mod = sys.modules["taskq.ratelimit.registry"]  # type: ignore[assignment]  # Why: as _registry

    reg = _registry()
    settings = _settings()
    ref = _pg_ref("audit-pubfail")
    # Materialize with no pool: the materialization arm never publishes.
    first = await reg._resolve_rate_limit_name(
        ref, _TenantPayload(tenant_id="k1"), settings=settings, pg_pool=None
    )
    assert first == "audit-pubfail:k1"
    assert first not in reg._keyed_rate_limit_row_schemas

    published: list[str] = []

    async def _failing_upsert(*args: object, **kwargs: object) -> None:
        published.append("attempted")
        raise RuntimeError("pg down")

    monkeypatch.setattr(registry_mod, "_upsert_rate_limit_bucket_row", _failing_upsert)
    second = await reg._resolve_rate_limit_name(
        ref, _TenantPayload(tenant_id="k1"), settings=settings, pg_pool=_FakePool()
    )
    assert second == first
    assert published == ["attempted"]
    # The capture rides the RESOLUTION, not the publish's outcome.
    assert reg._keyed_rate_limit_row_schemas[first] == "taskq_test"


async def test_upsert_rate_limit_bucket_row_schema_guard() -> None:
    """The shared row-writer re-validates the schema it interpolates —
    the same invariant the drain's guards pin, at the write seam."""
    from taskq.ratelimit.registry import (
        _upsert_rate_limit_bucket_row,  # pyright: ignore[reportPrivateUsage]
    )

    with pytest.raises(ValueError, match="invalid schema identifier"):
        await _upsert_rate_limit_bucket_row(
            None,  # type: ignore[arg-type]
            "bad schema; drop table",
            "bucket",
            "token_bucket",
            keyed=False,
        )


# ── the reservation's lifecycle-origin property ─────────────────────────


def test_concurrency_reservation_keyed_property() -> None:
    """The carried ``keyed`` mark is readable off the instance — the row
    writer's fleet-reclaimable stamp source."""
    from taskq.ratelimit.reservation import ConcurrencyReservation

    reservation = ConcurrencyReservation.__new__(ConcurrencyReservation)
    reservation._keyed = True
    assert reservation.keyed is True
    reservation._keyed = False
    assert reservation.keyed is False


# import last: the uuid7 seam the repo mandates
from taskq._ids import new_uuid  # noqa: E402
