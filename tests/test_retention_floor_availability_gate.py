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
behavior stay in their own packs, unchanged.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

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
