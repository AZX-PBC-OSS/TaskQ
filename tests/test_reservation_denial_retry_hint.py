"""A slot denial's retry hint is capacity-aware, not a flat constant.

A bucket with every slot held used to deny with the flat
``DEFAULT_RESERVATION_BACKOFF`` (5 s) no matter when capacity actually
frees: a bucket whose earliest lease expires in 30 s was retried every
5 s — six claim + acquire + snooze round trips that can only ever be
denied again, the denial-heavy loop measured at 12.3:1 denial:success.
The acquire already visits the held rows; the denial branch now reports
the earliest held lease's expiry as the ``retry_after`` hint (plus
:data:`RESERVATION_RETRY_HINT_MARGIN`), computed against the same
server clock that stamps the leases, folded into the SAME acquire
statement so the round-trip count is unchanged. With no live-held rows
to read (every row free but row-locked by a peer acquire — the SKIP
LOCKED case — or the anomalous held-without-lease state) the hint is
NULL and the flat constant remains the fallback.

The in-memory twin computes the same hint from its injected clock, so
the unit lane pins the arithmetic and the PG lane pins the SQL.
"""

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.constants import DEFAULT_RESERVATION_BACKOFF, RESERVATION_RETRY_HINT_MARGIN
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.reservation import (  # pyright: ignore[reportPrivateUsage]  # Why: the in-memory slot table is the unit under test; no public seam drives a denial without a full reservation stack.
    ConcurrencyReservation,
    _InMemorySlotTable,
)
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema

_START = datetime(2025, 1, 1, tzinfo=UTC)
_SHORT_LEASE = timedelta(seconds=10)
_LONG_LEASE = timedelta(seconds=30)
_TOLERANCE = timedelta(milliseconds=250)


# ── In-memory twin: the hint tracks the earliest held lease ─────────────


async def test_denied_acquire_retry_after_tracks_earliest_held_lease_expiry() -> None:
    """A full bucket denies with the earliest held lease's remaining time
    plus the safety margin — the denied job re-attempts when capacity can
    actually free, not on a flat cadence."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 2)
    table.acquire("gpu", new_uuid(), new_uuid(), _SHORT_LEASE)
    table.acquire("gpu", new_uuid(), new_uuid(), _LONG_LEASE)

    with pytest.raises(ReservationUnavailable) as exc_info:
        table.acquire("gpu", new_uuid(), new_uuid(), _SHORT_LEASE)

    assert exc_info.value.retry_after == _SHORT_LEASE + RESERVATION_RETRY_HINT_MARGIN, (
        f"a denial on a bucket whose earliest lease expires in {_SHORT_LEASE} "
        f"reported {exc_info.value.retry_after!r}; the hint must be the "
        "earliest held expiry (plus the safety margin) so the re-attempt "
        "lands when a slot can actually free."
    )


async def test_denied_acquire_reports_earliest_not_latest_lease() -> None:
    """Two holders with different lease horizons: the EARLIEST expiry is
    the hint — the slot that frees first is the one the re-attempt can
    win. Reporting the later lease (or any aggregation above the min)
    parks the job past real availability."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 2)
    table.acquire("gpu", new_uuid(), new_uuid(), _LONG_LEASE)
    table.acquire("gpu", new_uuid(), new_uuid(), _SHORT_LEASE)

    with pytest.raises(ReservationUnavailable) as exc_info:
        table.acquire("gpu", new_uuid(), new_uuid(), _SHORT_LEASE)

    assert exc_info.value.retry_after == _SHORT_LEASE + RESERVATION_RETRY_HINT_MARGIN


# ── The PG acquire statement's denial return, decoded ───────────────────


class _FakeConn:
    """ConnLike stand-in returning one canned row for the acquire's
    single statement — the new acquire shape always returns exactly one
    row (acquired fields NULL on the denial branch), so the decode is
    what these tests pin."""

    def __init__(self, row: dict[str, object] | None) -> None:
        self._row = row
        self.transactions_opened = 0

    async def fetchrow(self, _sql: str, *_params: object) -> dict[str, object] | None:
        return self._row

    def transaction(self) -> "_NullTxn":
        self.transactions_opened += 1
        return _NullTxn()


class _NullTxn:
    async def __aenter__(self) -> "_NullTxn":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeAcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class _FakePgPool:
    def __init__(self, row: dict[str, object] | None) -> None:
        self._conn = _FakeConn(row)

    # Why: mirrors asyncpg.Pool.acquire's signature (the #293 forward
    # contract under test).
    def acquire(self, *, timeout: float | None = None) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(self._conn)


def _reservation() -> ConcurrencyReservation:
    return ConcurrencyReservation(
        name="gpu",
        slots=2,
        lease=_SHORT_LEASE,
        schema="taskq",
    )


async def test_pg_denial_row_with_hint_decodes_to_expiry_plus_margin() -> None:
    """The denial branch's row (acquired fields NULL, hint seconds
    present) becomes ``timedelta(seconds) + margin`` on the raised
    ``ReservationUnavailable``."""
    res = _reservation()
    pool: Any = _FakePgPool({"slot_index": None, "acquired_at": None, "retry_after_seconds": 7.5})

    with pytest.raises(ReservationUnavailable) as exc_info:
        await res.acquire(new_uuid(), new_uuid(), pool)

    assert exc_info.value.retry_after == timedelta(seconds=7.5) + RESERVATION_RETRY_HINT_MARGIN


async def test_pg_denial_row_without_hint_falls_back_to_constant() -> None:
    """A NULL hint (no live-held rows — the SKIP LOCKED case) is the flat
    ``DEFAULT_RESERVATION_BACKOFF``, never a crash on the None and never
    a zero delay."""
    res = _reservation()
    pool: Any = _FakePgPool({"slot_index": None, "acquired_at": None, "retry_after_seconds": None})

    with pytest.raises(ReservationUnavailable) as exc_info:
        await res.acquire(new_uuid(), new_uuid(), pool)

    assert exc_info.value.retry_after == DEFAULT_RESERVATION_BACKOFF


async def test_pg_acquired_row_ignores_hint_and_returns_slot_lease() -> None:
    """The acquired branch is unchanged by the hint column: a non-NULL
    ``slot_index`` returns the :class:`SlotLease` exactly as before."""
    acquired_at = datetime(2025, 1, 1, tzinfo=UTC)
    res = _reservation()
    pool: Any = _FakePgPool(
        {"slot_index": 3, "acquired_at": acquired_at, "retry_after_seconds": 7.5}
    )

    lease = await res.acquire(new_uuid(), new_uuid(), pool)

    assert int(lease) == 3
    assert lease.acquired_at == acquired_at


async def test_pg_row_none_denial_falls_back_to_constant() -> None:
    """The LEFT JOIN shape always yields one row; a driver-level None is
    the defensive branch and must still deny with the constant rather
    than crash."""
    res = _reservation()
    pool: Any = _FakePgPool(None)

    with pytest.raises(ReservationUnavailable) as exc_info:
        await res.acquire(new_uuid(), new_uuid(), pool)

    assert exc_info.value.retry_after == DEFAULT_RESERVATION_BACKOFF


# ── Real Postgres: the hint tracks actual lease state ───────────────────


class TestReservationDenialHintPg:
    """The capacity-aware denial against real PG — the SQL's
    ``earliest_held`` computation and its clock domain only exist
    server-side."""

    pytestmark = pytest.mark.integration

    async def test_denial_retry_hint_tracks_earliest_held_lease(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """A single-slot bucket held under a 30 s lease denies with
        (expiry - now) + margin — computed on the server clock in the
        same statement, so the tolerance is only the denial's own
        latency, not clock skew."""
        schema = module_pg_schema.schema_name
        bucket = f"hint_{new_base62()}"
        res = ConcurrencyReservation(name=bucket, slots=1, lease=_LONG_LEASE, schema=schema)
        await res.ensure_slots(module_pg_pool)
        await res.acquire(new_uuid(), new_uuid(), module_pg_pool)

        async with module_pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT lease_expires_at, clock_timestamp() AS now "  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                f'FROM "{schema}".reservation_slots WHERE bucket_name = $1',
                bucket,
            )
        assert row is not None
        expected = (row["lease_expires_at"] - row["now"]) + RESERVATION_RETRY_HINT_MARGIN

        with pytest.raises(ReservationUnavailable) as exc_info:
            await res.acquire(new_uuid(), new_uuid(), module_pg_pool)

        assert exc_info.value.retry_after == pytest.approx(expected, abs=_TOLERANCE), (
            f"a denial on a bucket whose lease expires in ~{_LONG_LEASE} reported "
            f"{exc_info.value.retry_after!r}; the hint must track the earliest "
            "held expiry so the re-attempt lands when the slot can free."
        )

    async def test_denial_retry_hint_reports_earliest_of_two_leases(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """Two holders, short and long lease: the SHORT one's expiry is
        the hint — the earliest expiry is the capacity that frees first."""
        schema = module_pg_schema.schema_name
        bucket = f"hint_two_{new_base62()}"
        res_short = ConcurrencyReservation(name=bucket, slots=2, lease=_SHORT_LEASE, schema=schema)
        res_long = ConcurrencyReservation(name=bucket, slots=2, lease=_LONG_LEASE, schema=schema)
        await res_short.ensure_slots(module_pg_pool)
        await res_short.acquire(new_uuid(), new_uuid(), module_pg_pool)
        await res_long.acquire(new_uuid(), new_uuid(), module_pg_pool)

        async with module_pg_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT min(lease_expires_at) AS earliest, clock_timestamp() AS now "  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                f'FROM "{schema}".reservation_slots WHERE bucket_name = $1',
                bucket,
            )
        assert row is not None
        expected = (row["earliest"] - row["now"]) + RESERVATION_RETRY_HINT_MARGIN

        with pytest.raises(ReservationUnavailable) as exc_info:
            await res_short.acquire(new_uuid(), new_uuid(), module_pg_pool)

        assert exc_info.value.retry_after == pytest.approx(expected, abs=_TOLERANCE)

    async def test_denial_with_locked_free_row_falls_back_to_constant(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """A bucket whose only row is FREE but row-locked by a peer
        acquire (FOR UPDATE SKIP LOCKED skips it) has no live-held lease
        to read: the hint is NULL and the denial carries the flat
        ``DEFAULT_RESERVATION_BACKOFF``."""
        schema = module_pg_schema.schema_name
        bucket = f"hint_locked_{new_base62()}"
        res = ConcurrencyReservation(name=bucket, slots=1, lease=_LONG_LEASE, schema=schema)
        await res.ensure_slots(module_pg_pool)

        holder = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            async with holder.transaction():
                await holder.execute(
                    f'SELECT slot_index FROM "{schema}".reservation_slots '  # noqa: S608  # Why: schema is fixture-derived; bucket_name is $1-bound
                    "WHERE bucket_name = $1 FOR UPDATE",
                    bucket,
                )
                with pytest.raises(ReservationUnavailable) as exc_info:
                    await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
        finally:
            await holder.close()

        assert exc_info.value.retry_after == DEFAULT_RESERVATION_BACKOFF
        # The held row lock released with the peer's transaction: the
        # next acquire takes the slot normally.
        lease = await res.acquire(new_uuid(), new_uuid(), module_pg_pool)
        assert int(lease) == 0


async def test_pg_acquire_runs_its_single_statement_without_an_explicit_transaction() -> None:
    """The acquire is one data-modifying-CTE statement and a single
    statement is atomic on its own; wrapping it in ``conn.transaction()``
    only adds a BEGIN and a COMMIT round trip per reserved job."""
    res = _reservation()
    pool = _FakePgPool({"slot_index": 0, "acquired_at": _START, "retry_after_seconds": None})

    await res.acquire(new_uuid(), new_uuid(), cast(Any, pool))

    assert pool._conn.transactions_opened == 0  # pyright: ignore[reportPrivateUsage]  # Why: the fake's own recorded state.
