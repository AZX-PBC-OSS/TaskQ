"""The retention floor's availability gate: the TimescaleDB pre-check and
the negative's cache.

On a server WITHOUT the ``timescaledb`` extension — managed vanilla-
Postgres offerings above all — the floor's catalog probe can never
succeed, and before the gate it was ISSUED on every sweep: a round trip
that always fails, an ``UndefinedTableError`` recorded as an exception
event on the DB span each time (tens of thousands per month from one
idle worker), alert fatigue with no rate limit and no cache.

The gate: the floor asks ``pg_extension`` FIRST (unprivileged, one
catalog read), caches the negative answer per connection (pooled
connections are long-lived, so per-connection is per-pool in effect;
extensions cannot be removed mid-connection in practice), and answers
None — the caller's full-range sweep, byte-identical to vanilla —
without ever issuing a ``timescaledb_information`` statement on such a
server. An ambiguous answer (the pre-check itself erroring) is never
cached: the next sweep re-asks, and the fail-open contract is
unchanged.

These pins are hermetic (a recording stub conn counts the statements on
the wire); the Timescale-present path's parse teeth and live-engine
behavior stay in their own packs, unchanged. The conn-TYPE pins are the
exception: the sweeps hand the floor asyncpg's ``PoolConnectionProxy``
(a ``__slots__``-only wrapper, minted fresh per acquire), so the pack
pins that wrapper's shape too — hermetically (a slots stub mirroring
it) and end-to-end (a real pool acquire on the self-provisioned
vanilla server) — because a gate that only works on weak-referenceable
stubs is a gate that kills the production sweep loop.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._retention_floor import retention_policy_floor
from taskq.backend._sweeps import sweep_expired_events

_TEST_EVENT_RETENTION = timedelta(seconds=3600)  # the event sweep's retention bound


class _RecordingConn:
    """A stub conn that records every statement and routes the two
    probes: the ``pg_extension`` availability check and the
    ``timescaledb_information`` floor probe — each with a configured
    answer (rows, an exception, or an unhandled fallthrough)."""

    def __init__(
        self,
        *,
        availability_rows: list[dict[str, int]] | None = None,
        availability_error: Exception | None = None,
        probe_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.executes: list[tuple[str, tuple[object, ...]]] = []
        self._availability_rows = availability_rows
        self._availability_error = availability_error
        self._probe_rows = probe_rows if probe_rows is not None else []

    async def fetch(self, sql: str, *args: object) -> list[dict[str, Any]]:
        self.statements.append(sql)
        if "pg_extension" in sql:
            if self._availability_error is not None:
                raise self._availability_error
            return self._availability_rows if self._availability_rows is not None else []
        if "timescaledb_information" in sql:
            return self._probe_rows
        raise AssertionError(f"unexpected statement reached the wire: {sql[:120]}")

    async def execute(self, sql: str, *args: object) -> str:
        self.statements.append(sql)
        self.executes.append((sql, args))
        return "DELETE 0"

    def tsdb_statements(self) -> list[str]:
        return [s for s in self.statements if "timescaledb_information" in s]

    def availability_statements(self) -> list[str]:
        return [s for s in self.statements if "pg_extension" in s]


class _SlotsProxyConn:
    """asyncpg's production acquire shape, mirrored: the pool hands out
    ``PoolConnectionProxy`` — ``__slots__``-only (no ``__weakref__``),
    minted fresh per acquire, wrapping the long-lived underlying
    connection."""

    __slots__ = ("_con",)

    def __init__(self, inner: _RecordingConn) -> None:
        self._con = inner

    async def fetch(self, sql: str, *args: object) -> list[dict[str, Any]]:
        return await self._con.fetch(sql, *args)

    async def execute(self, sql: str, *args: object) -> str:
        return await self._con.execute(sql, *args)

    def tsdb_statements(self) -> list[str]:
        return self._con.tsdb_statements()

    def availability_statements(self) -> list[str]:
        return self._con.availability_statements()


class _UnkeyableConn:
    """A wrapper that can neither be unwrapped (no ``_con``) nor weakly
    referenced (``__slots__`` without ``__weakref__``): the shape the
    cache must answer without memoizing."""

    __slots__ = ("_statements",)

    def __init__(self) -> None:
        self._statements: list[str] = []

    async def fetch(self, sql: str, *args: object) -> list[dict[str, int]]:
        self._statements.append(sql)
        if "pg_extension" in sql:
            return []
        if "timescaledb_information" in sql:
            return []
        raise AssertionError(f"unexpected statement reached the wire: {sql[:120]}")

    async def execute(self, sql: str, *args: object) -> str:
        self._statements.append(sql)
        return "DELETE 0"

    def tsdb_statements(self) -> list[str]:
        return [s for s in self._statements if "timescaledb_information" in s]

    def availability_statements(self) -> list[str]:
        return [s for s in self._statements if "pg_extension" in s]


async def test_vanilla_server_never_issues_the_catalog_probe_and_caches_the_negative() -> None:
    """On a server without the extension the floor answers None on every
    call while the ``timescaledb_information`` statement is issued ZERO
    times — the availability pre-check answers instead, and its negative
    is cached per connection: the pre-check itself runs once, not once
    per sweep."""
    conn = _RecordingConn()  # availability_rows defaults to [] — vanilla

    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert conn.tsdb_statements() == [], (
        "the catalog probe must never reach a server without the extension"
    )
    assert len(conn.availability_statements()) == 1, (
        "the first call pays exactly one availability read"
    )

    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert await retention_policy_floor(conn, "v_schema", "jobs_archive", "finished_at") is None
    assert conn.tsdb_statements() == [], (
        "the cached negative must keep the catalog probe off the wire"
    )
    assert len(conn.availability_statements()) == 1, (
        "the negative is cached per connection: the later sweeps re-ask nothing"
    )


async def test_the_availability_check_is_cheap_and_the_sweep_issues_zero_catalog_statements() -> (
    None
):
    """The issue's sweep-level shape: a full sweep on a vanilla server
    issues the availability read and its one full-range DELETE — zero
    ``timescaledb_information`` statements on the wire."""
    conn = _RecordingConn()

    deleted = await sweep_expired_events(
        conn, schema="v_schema", retention=_TEST_EVENT_RETENTION, batch_size=100
    )
    assert deleted == 0
    assert conn.tsdb_statements() == [], (
        "a sweep on a vanilla server must not issue the catalog probe — "
        "the availability gate answers first"
    )
    assert len(conn.availability_statements()) == 1
    [(sql, args)] = conn.executes
    assert len(args) == 2, (
        "full-range: only the retention bound and the batch LIMIT ride as "
        "parameters — the floor's $3 must be absent when the gate answers "
        "no extension"
    )
    assert "occurred_at >= $3" not in sql, (
        "full-range: the floor conjunct must be absent from the rendered statement"
    )


async def test_an_ambiguous_availability_answer_is_never_cached() -> None:
    """A failing pre-check is ambiguity, not an answer: the floor fails
    open to None and does NOT cache — the next call re-asks (today's
    per-sweep behavior for anything uncertain), so a transient error can
    never pin a wrong availability verdict on the connection."""
    conn = _RecordingConn(availability_error=RuntimeError("the catalog is unreachable"))

    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert conn.tsdb_statements() == []
    assert len(conn.availability_statements()) == 2, (
        "the ambiguous answer is never cached: each call re-asks"
    )


async def test_the_present_path_reaches_the_probe_and_its_parse_teeth() -> None:
    """On a server WITH the extension the gate opens and the floor probe
    runs exactly as before — its parse fail-open (a row without the
    probe's own keys reads as 'no answer') stays reachable through the
    gate: the pre-check is an optimization and a noise cure, never a
    semantic change."""
    conn = _RecordingConn(
        availability_rows=[{"?column?": 1}],
        probe_rows=[{"rows_examined": 3}],  # no 'drop_after', no 'db_now'
    )

    assert await retention_policy_floor(conn, "t_schema", "job_events", "occurred_at") is None, (
        "the parse-layer fail-open still holds behind the gate"
    )
    assert len(conn.tsdb_statements()) == 1, "the present path issues the catalog probe"
    assert len(conn.availability_statements()) == 1

    # The present answer caches the AVAILABILITY read only: the floor
    # probe itself stays per-call (one catalog question per sweep run,
    # as documented) — the second call skips the pre-check and issues
    # the probe again.
    assert await retention_policy_floor(conn, "t_schema", "job_events", "occurred_at") is None
    assert len(conn.tsdb_statements()) == 2, (
        "the cached positive skips the pre-check, never the probe: the "
        "catalog question stays per-call on the present path"
    )
    assert len(conn.availability_statements()) == 1


async def test_the_production_proxy_shape_is_answered_not_raised() -> None:
    """The sweeps hand the floor the pool's proxy — a __slots__-only
    wrapper with no __weakref__. The gate must ANSWER such a caller (the
    fail-open contract knows no exception for the wrapper's type), and it
    must key the cache on the UNDERLYING connection: two acquires mint
    two proxies sharing one connection, and the second sweep must not
    re-ask."""
    inner = _RecordingConn()  # vanilla
    first = _SlotsProxyConn(inner)
    second = _SlotsProxyConn(inner)  # a fresh acquire over the SAME connection

    assert await retention_policy_floor(first, "v_schema", "job_events", "occurred_at") is None, (
        "the proxy's type must be answered, never a TypeError through the sweep"
    )
    assert await retention_policy_floor(second, "v_schema", "job_events", "occurred_at") is None
    assert first.tsdb_statements() == [] and second.tsdb_statements() == []
    assert len(inner.availability_statements()) == 1, (
        "the cache keys the underlying connection: a fresh proxy per acquire must not re-ask"
    )


async def test_an_unkeyable_conn_still_fails_open_and_gates_the_probe() -> None:
    """A wrapper that can neither be unwrapped (no ``_con``) nor weakly
    referenced must be ANSWERED — never a TypeError — and its answer is
    simply not memoized: each call re-asks the cheap pre-check, and the
    catalog probe stays equally off the wire. The guarantee that never
    degrades is zero catalog-probe statements; only the memoization is
    conditional on a key existing."""
    conn = _UnkeyableConn()

    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert conn.tsdb_statements() == []
    assert len(conn.availability_statements()) == 2, (
        "unkeyable: no memoization — each call re-asks, and the answer is per-call correct"
    )


async def test_recycled_connections_re_ask() -> None:
    """A pool's recycle mints a NEW underlying connection; the cached
    answer does not survive it, and the new connection's first sweep
    re-asks (a recycled endpoint can be a different server)."""
    inner_one = _RecordingConn()
    assert (
        await retention_policy_floor(
            _SlotsProxyConn(inner_one), "v_schema", "job_events", "occurred_at"
        )
        is None
    )
    assert len(inner_one.availability_statements()) == 1

    del inner_one

    inner_two = _RecordingConn()
    assert (
        await retention_policy_floor(
            _SlotsProxyConn(inner_two), "v_schema", "job_events", "occurred_at"
        )
        is None
    )
    assert len(inner_two.availability_statements()) == 1, (
        "the recycled connection's first sweep re-asks: the cached "
        "answer died with the recycled connection"
    )


async def test_two_servers_key_their_own_answers() -> None:
    """A multi-server process (the test matrix, a multi-DSN deployment)
    keys per underlying connection: one vanilla and one Timescale-present
    server each hold their own answer — the vanilla side never issues
    the catalog probe, the present side runs it."""
    vanilla_inner = _RecordingConn()
    present_inner = _RecordingConn(
        availability_rows=[{"?column?": 1}],
        probe_rows=[{"rows_examined": 3}],
    )

    assert (
        await retention_policy_floor(
            _SlotsProxyConn(vanilla_inner), "s", "job_events", "occurred_at"
        )
        is None
    )
    assert (
        await retention_policy_floor(
            _SlotsProxyConn(present_inner), "s", "job_events", "occurred_at"
        )
        is None
    )

    assert vanilla_inner.tsdb_statements() == [], "the vanilla server keeps the probe off the wire"
    assert len(present_inner.tsdb_statements()) == 1, (
        "the present server's probe runs under its own cached answer"
    )


async def test_the_enable_path_busts_cached_negatives() -> None:
    """The supported way the extension appears under a live connection is
    the deploy-step conversion; its enable path busts the cache the
    moment the server is confirmed Timescale, so no cached negative
    outlives the install on the supported path."""
    from taskq.backend._retention_floor import (  # pyright: ignore[reportPrivateUsage]
        clear_timescaledb_availability_cache,
    )

    conn = _RecordingConn()
    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert len(conn.availability_statements()) == 1

    clear_timescaledb_availability_cache()

    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert len(conn.availability_statements()) == 2, (
        "the bust forces the re-ask: a cached negative cannot outlive the enable path"
    )


async def test_real_pool_acquire_shape_never_raises(pg_dsn: str) -> None:
    """The production conn type end-to-end: ``pool.acquire()`` hands the
    floor a real ``PoolConnectionProxy``; the floor must answer None on a
    vanilla server — no raise, no catalog probe — and the second call
    must ride the cached negative on the underlying connection."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        async with pool.acquire() as conn:
            schema = "rfg_" + new_uuid().hex[:12]
            assert await retention_policy_floor(conn, schema, "job_events", "occurred_at") is None
            assert await retention_policy_floor(conn, schema, "job_events", "occurred_at") is None
    finally:
        await pool.close()


async def test_a_stale_answer_re_asks_when_its_ttl_passes(
    monkeypatch: Any,
) -> None:
    """The cached answer (negative AND positive) is bounded: past the
    TTL the connection re-asks — a staleness bound that holds regardless
    of process boundaries, pool recycling, or who installed the
    extension. The clock is the module's own seam (monotonic), and the
    boundary pins the stated value: fresh through 300s, re-asked past
    it."""
    import taskq.backend._retention_floor as floor_mod

    clock = {"now": 1000.0}
    monkeypatch.setattr(floor_mod, "_MONOTONIC", lambda: clock["now"])
    vanilla = _RecordingConn()  # vanilla: the negative path
    present = _RecordingConn(
        availability_rows=[{"?column?": 1}],
        probe_rows=[{"rows_examined": 3}],
    )  # the positive path

    assert await retention_policy_floor(vanilla, "v_schema", "job_events", "occurred_at") is None
    assert await retention_policy_floor(present, "v_schema", "job_events", "occurred_at") is None
    assert len(vanilla.availability_statements()) == 1
    assert len(present.availability_statements()) == 1

    clock["now"] += 299.0  # fresh inside the TTL: neither polarity re-asks
    assert await retention_policy_floor(vanilla, "v_schema", "job_events", "occurred_at") is None
    assert await retention_policy_floor(present, "v_schema", "job_events", "occurred_at") is None
    assert len(vanilla.availability_statements()) == 1
    assert len(present.availability_statements()) == 1

    clock["now"] += (
        1.0  # EXACTLY at the stated bound: still fresh (the answer's age is 300, the bound is inclusive)
    )
    assert await retention_policy_floor(vanilla, "v_schema", "job_events", "occurred_at") is None
    assert await retention_policy_floor(present, "v_schema", "job_events", "occurred_at") is None
    assert len(vanilla.availability_statements()) == 1, (
        "the stated value is pinned: fresh THROUGH 300s (the bound is "
        "inclusive), not through 299.5 and not exclusive"
    )
    assert len(present.availability_statements()) == 1

    clock["now"] += 0.5  # past the stated bound: both polarities re-ask
    assert await retention_policy_floor(vanilla, "v_schema", "job_events", "occurred_at") is None
    assert await retention_policy_floor(present, "v_schema", "job_events", "occurred_at") is None
    assert len(vanilla.availability_statements()) == 2, (
        "the negative is a bound, not a promise: past the TTL the "
        "connection re-asks — the staleness cure that holds across "
        "processes"
    )
    assert len(present.availability_statements()) == 2, (
        "the positive is bounded by the same TTL: a server that lost the "
        "extension cannot be trusted from a stale answer past the bound"
    )


async def test_an_unhashable_underlying_connection_is_answered_not_raised() -> None:
    """The fail-open covers the WHOLE cache interaction: an underlying
    connection that is weak-referenceable but unhashable (or any cache
    operation that raises) is answered None, uncached — the cache can
    never raise a TypeError through the sweep, the exact shape the
    first cut shipped."""

    class _UnhashableInner(_RecordingConn):
        __hash__ = None  # type: ignore[assignment]

    conn = _SlotsProxyConn(_UnhashableInner())

    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None, (
        "an unhashable underlying connection is answered, never a TypeError through the sweep"
    )
    assert await retention_policy_floor(conn, "v_schema", "job_events", "occurred_at") is None
    assert conn.tsdb_statements() == [], "the probe stays gated for every caller shape"
