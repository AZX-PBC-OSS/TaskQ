# ruff: noqa: S608  # Why: every schema interpolation is a fixed per-run test identifier, and every value is $-bound.
"""The progress flush's atomicity pins: one transaction per durable
group, the knock only after the commit.

The defect these pins hold shut: the flush's three durable statements
(the stream append/trim, the progress-state upsert, the stream-counter
upsert) are one logical record. As separate autocommits, a cancellation
or failure between them commits a fragment: a stream row the state row
never counts, or a state row the stream counters never count, with the
buffer already cleared, so neither the final close nor the finalize can
repair it. After an SSE reconnect the displayed progress permanently
regresses; the occurrence and drop counters lie. The cure wraps each
durable group in one ``conn.transaction()`` and knocks only after the
commit: a loss before the append loses the whole snapshot (allowed),
a cancellation after it loses nothing durable (only the pointer).

The boundaries are exercised with REAL row locks (``FOR UPDATE`` held
by a second connection) and REAL cancellations of the real methods, so
the old fragmenting shape is caught at its own commit boundaries; a
transaction-aware recording fake proves the knock ordering. The
fail-closed discipline: an unrecognized statement fails the fake.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._progress import (
    KIND_NODE_STARTED,
    PROGRESS_NOTIFY_CHANNEL,
    STREAM_CHANNEL,
    ProgressEmitter,
    project_auto_event,
)
from tests._wf_fixtures import seed_flow, seed_running_node

pytestmark = pytest.mark.integration


async def _reap_quietly(emitter: ProgressEmitter) -> None:
    """The pins' own containment: reap the emitter's cadence task
    whatever the flush under test did, so a red run leaks nothing."""
    task = emitter._flush_task
    if task is not None and not task.done():
        task.cancel()
    if task is not None:
        with contextlib.suppress(asyncio.CancelledError):
            await task


class _Journal:
    """The recording fake's ledger: the protocol's counts, never a
    permissive pass-through."""

    def __init__(self) -> None:
        self.begun = 0
        self.committed = 0
        self.rolled_back = 0
        self.appended = 0
        self.state_upserts = 0
        self.stream_counters = 0
        self.knocks_started = 0
        self.knocks_done = 0


class _FakeTx:
    def __init__(self, journal: _Journal) -> None:
        self._journal = journal

    async def __aenter__(self) -> object:
        self._journal.begun += 1
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        if exc_type is None:
            self._journal.committed += 1
        else:
            self._journal.rolled_back += 1
        return False


class _FakeConn:
    """Records the flush's statements and fails closed on any statement
    outside the flush's durable group or knock."""

    def __init__(
        self, journal: _Journal, knock_gate: asyncio.Event, knock_started: asyncio.Event
    ) -> None:
        self._journal = journal
        self._knock_gate = knock_gate
        self._knock_started = knock_started

    def transaction(self) -> _FakeTx:
        return _FakeTx(self._journal)

    async def fetchrow(self, sql: str, *args: object) -> dict[str, int]:
        assert "wf_node_stream" in sql, f"unexpected statement: {sql[:120]}"
        self._journal.appended += 1
        return {"seq": self._journal.appended, "dropped": 0}

    async def execute(self, sql: str, *args: object) -> str:
        if "pg_notify" in sql:
            self._journal.knocks_started += 1
            self._knock_started.set()
            await self._knock_gate.wait()
            self._journal.knocks_done += 1
            return "NOTIFY"
        if "wf_node_progress" in sql:
            if args[1] == STREAM_CHANNEL:
                self._journal.stream_counters += 1
            else:
                self._journal.state_upserts += 1
            return "UPDATE 1"
        raise AssertionError(f"unmatched statement: {sql[:120]}")


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


class _FakeWsql:
    """The statement bundle's stand-in: the real attribute surface with
    the table names the fake's fail-closed matching keys on."""

    schema = "wf_test_schema"
    progress_stream_append = "INSERT INTO wf_test_schema.wf_node_stream ..."
    progress_state_upsert = "INSERT INTO wf_test_schema.wf_node_progress ... ON CONFLICT ..."


def _fake_emitter(
    knock_gate: asyncio.Event, knock_started: asyncio.Event
) -> tuple[ProgressEmitter, _Journal]:
    journal = _Journal()
    emitter = ProgressEmitter(
        _FakePool(_FakeConn(journal, knock_gate, knock_started)),  # type: ignore[arg-type]
        _FakeWsql(),  # type: ignore[arg-type]
        flow_id=new_uuid(),  # type: ignore[arg-type]
        node_id=new_uuid(),  # type: ignore[arg-type]
        cadence_s=0,
    )
    return emitter, journal


async def _hold_progress_row(
    pool: asyncpg.Pool,
    schema: str,
    node_id: JobId,
    channel: str,
    ready: asyncio.Event,
    release: asyncio.Event,
    xid_out: list[str],
) -> None:
    """Hold a real row lock on the upsert's target row from a second
    connection: the flush's durable statement blocks on it for real.
    The holder's own transaction id is published before ready, so the
    block detection matches THIS blocker and nothing else."""
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            f'SELECT 1 FROM "{schema}".wf_node_progress WHERE node_id=$1 AND channel=$2 FOR UPDATE',
            node_id,
            channel,
        )
        xid_out.append(str(await conn.fetchval("SELECT pg_current_xact_id()::text::bigint")))
        ready.set()
        await release.wait()


async def _wait_for_block(pool: asyncpg.Pool, xid: str, deadline_s: float = 10.0) -> None:
    """Deterministic block detection scoped to THIS blocker: the flush's
    upsert is waiting for exactly the holder's transaction to release
    it (an ungranted transactionid lock on the holder's own xid)."""
    async with pool.acquire() as conn:
        deadline = asyncio.get_running_loop().time() + deadline_s
        while asyncio.get_running_loop().time() < deadline:
            waiting = await conn.fetchval(
                "SELECT count(*) FROM pg_locks "
                "WHERE NOT granted AND locktype = 'transactionid' "
                "AND transactionid::text = $1",
                xid,
            )
            if waiting:
                return
            await asyncio.sleep(0.02)
    raise AssertionError("the flush never blocked on the held row lock")


async def _progress_row(
    conn: asyncpg.Connection, schema: str, node_id: JobId, channel: str
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f'SELECT occurrences, dropped, last_seq, pct, message FROM "{schema}".wf_node_progress '
        "WHERE node_id=$1 AND channel=$2",
        node_id,
        channel,
    )


async def _stream_count(conn: asyncpg.Connection, schema: str, node_id: JobId) -> int:
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_node_stream WHERE node_id=$1',
            node_id,
        )
    )


async def _seed_prior_state(
    emitter: ProgressEmitter, conn: asyncpg.Connection, schema: str, node_id: JobId
) -> tuple[int, int, int]:
    """One real flush as the PRIOR state: the coherent record the
    interrupted second flush must never fragment."""
    await emitter.emit(10, "prior", None)
    assert emitter._flush_task is not None
    await asyncio.wait_for(emitter._flush_task, timeout=10.0)
    row = await _progress_row(conn, schema, node_id, "progress")
    assert row is not None, "the prior flush left no state row"
    prior = (int(row["occurrences"]), int(row["dropped"]), int(row["last_seq"]))
    assert await _stream_count(conn, schema, node_id) == 1
    counters = await _progress_row(conn, schema, node_id, STREAM_CHANNEL)
    assert counters is not None and int(counters["occurrences"]) == 1
    return prior


async def test_flush_rolls_back_the_stream_append_when_the_state_upsert_blocks(
    wf_pool: asyncpg.Pool, wf_sql: Any, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The boundary between the stream append and the progress-state
    upsert: the interrupted flush must leave the exact prior state (the
    append rolls back with the transaction), never a stream row the
    state row never counts."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0)
    prior_occ, prior_dropped, prior_seq = await _seed_prior_state(
        emitter, wf_conn, wf_schema, node_id
    )
    ready, release = asyncio.Event(), asyncio.Event()
    xid_out: list[str] = []
    blocker = asyncio.create_task(
        _hold_progress_row(wf_pool, wf_schema, node_id, "progress", ready, release, xid_out)
    )
    await asyncio.wait_for(ready.wait(), timeout=5.0)
    try:
        await emitter.emit(40, "interrupted", None)
        flush_task = emitter._flush_task
        assert flush_task is not None, "the cadence flush was not armed"
        await _wait_for_block(wf_pool, xid_out[0])
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task
        # The fragment the old autocommit shape committed: a second
        # stream row while the state row still says the prior record.
        assert await _stream_count(wf_conn, wf_schema, node_id) == 1, (
            "the append committed without the state upsert: a stream row "
            "the progress state never counted"
        )
        row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert row is not None
        assert (
            int(row["occurrences"]),
            int(row["dropped"]),
            int(row["last_seq"]),
        ) == (prior_occ, prior_dropped, prior_seq), "the state row moved without its durable group"
        counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
        assert counters is not None
        assert int(counters["occurrences"]) == 1, "the counters moved without their group"
    finally:
        release.set()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker
        await _reap_quietly(emitter)


async def test_flush_rolls_back_state_and_append_when_the_counters_block(
    wf_pool: asyncpg.Pool, wf_sql: Any, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The boundary between the progress-state upsert and the
    stream-counter upsert: the interrupted flush must roll the whole
    group back, never a state row the stream counters do not count."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    emitter = ProgressEmitter(wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0)
    prior_occ, prior_dropped, prior_seq = await _seed_prior_state(
        emitter, wf_conn, wf_schema, node_id
    )
    ready, release = asyncio.Event(), asyncio.Event()
    xid_out: list[str] = []
    blocker = asyncio.create_task(
        _hold_progress_row(wf_pool, wf_schema, node_id, STREAM_CHANNEL, ready, release, xid_out)
    )
    await asyncio.wait_for(ready.wait(), timeout=5.0)
    try:
        await emitter.emit(60, "interrupted", None)
        flush_task = emitter._flush_task
        assert flush_task is not None, "the cadence flush was not armed"
        await _wait_for_block(wf_pool, xid_out[0])
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task
        assert await _stream_count(wf_conn, wf_schema, node_id) == 1, (
            "the append committed without the counters upsert"
        )
        row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert row is not None
        assert (
            int(row["occurrences"]),
            int(row["dropped"]),
            int(row["last_seq"]),
        ) == (prior_occ, prior_dropped, prior_seq), (
            "the state row committed without the stream counters row"
        )
        counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
        assert counters is not None
        assert (
            int(counters["occurrences"]),
            int(counters["dropped"]),
            int(counters["last_seq"]),
        ) == (prior_occ, prior_dropped, prior_seq), (
            "the counters row moved without its durable group"
        )
    finally:
        release.set()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker
        await _reap_quietly(emitter)


async def test_flush_rolls_back_the_drop_count_with_its_group(
    wf_pool: asyncpg.Pool, wf_sql: Any, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The emitter's in-memory counters move with the durable commit: a
    ring trim rolled back by the transaction leaves dropped_total at
    the prior value (the counter and the persistence agree), and the
    next successful flush's trim lands on both sides of the invariant
    (the persisted drop counter equals the emitter's total)."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    emitter = ProgressEmitter(
        wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0, ring_bound=1
    )
    prior_occ, prior_dropped, prior_seq = await _seed_prior_state(
        emitter, wf_conn, wf_schema, node_id
    )
    prior_dropped_total = emitter.dropped_total
    ready, release = asyncio.Event(), asyncio.Event()
    xid_out: list[str] = []
    blocker = asyncio.create_task(
        _hold_progress_row(wf_pool, wf_schema, node_id, "progress", ready, release, xid_out)
    )
    await asyncio.wait_for(ready.wait(), timeout=5.0)
    try:
        await emitter.emit(50, "trimmed", None)
        flush_task = emitter._flush_task
        assert flush_task is not None, "the cadence flush was not armed"
        await _wait_for_block(wf_pool, xid_out[0])
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task
        assert await _stream_count(wf_conn, wf_schema, node_id) == 1, (
            "the trimmed append committed without its durable group"
        )
        row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert row is not None
        assert (
            int(row["occurrences"]),
            int(row["dropped"]),
            int(row["last_seq"]),
        ) == (prior_occ, prior_dropped, prior_seq)
        assert emitter.dropped_total == prior_dropped_total, (
            "the in-memory drop total moved without its durable group: the "
            "counter and the persistence disagree"
        )
    finally:
        release.set()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker
    # The invariant across the rollback then a success: the next
    # flush's trim lands on the record and on the emitter's total
    # equally.
    await emitter.emit(70, "after", None)
    assert emitter._flush_task is not None
    await asyncio.wait_for(emitter._flush_task, timeout=10.0)
    counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
    assert counters is not None
    assert int(counters["dropped"]) == emitter.dropped_total, (
        "the shipped invariant broke: the persisted drop counter does not equal the emitter's total"
    )
    row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
    assert row is not None and int(row["occurrences"]) == prior_occ + 1


async def test_project_auto_event_rolls_back_the_append_when_the_counters_block(
    wf_pool: asyncpg.Pool, wf_sql: Any, wf_conn: asyncpg.Connection, wf_schema: str
) -> None:
    """The auto projection's boundary between its append and its
    stream-counter upsert: the interrupted projection must roll back to
    the exact prior state, never a stream row the counters do not
    count."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    prior_seq = await project_auto_event(
        wf_pool,
        wf_sql,
        flow_id=flow_id,
        node_id=node_id,
        kind=KIND_NODE_STARTED,
        payload={"step": "a"},
    )
    counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
    assert counters is not None and int(counters["occurrences"]) == 1
    ready, release = asyncio.Event(), asyncio.Event()
    xid_out: list[str] = []
    blocker = asyncio.create_task(
        _hold_progress_row(wf_pool, wf_schema, node_id, STREAM_CHANNEL, ready, release, xid_out)
    )
    await asyncio.wait_for(ready.wait(), timeout=5.0)
    projection = asyncio.create_task(
        project_auto_event(
            wf_pool,
            wf_sql,
            flow_id=flow_id,
            node_id=node_id,
            kind=KIND_NODE_STARTED,
            payload={"step": "b"},
        )
    )
    try:
        await _wait_for_block(wf_pool, xid_out[0])
        projection.cancel()
        with pytest.raises(asyncio.CancelledError):
            await projection
        assert await _stream_count(wf_conn, wf_schema, node_id) == 1, (
            "the auto projection's append committed without its counters upsert"
        )
        counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
        assert counters is not None
        assert (
            int(counters["occurrences"]),
            int(counters["dropped"]),
            int(counters["last_seq"]),
        ) == (1, 0, prior_seq), "the counters moved without their durable group"
    finally:
        release.set()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker


async def test_flush_knocks_only_after_the_durable_commit() -> None:
    """The knock is the pointer, not the record: it must not start
    before the durable group's commit, and the emitter's stream-append
    counter moves with the commit, not with the knock. A cancellation
    during the knock loses only the pointer; the durable state and the
    counter are already coherent. The pin drives the cadence task emit
    armed, never a second racing flush task."""
    knock_gate = asyncio.Event()
    knock_started = asyncio.Event()
    emitter, journal = _fake_emitter(knock_gate, knock_started)
    await emitter.emit(40, "knock me", None)
    flush_task = emitter._flush_task
    assert flush_task is not None, "the cadence flush was not armed"
    try:
        await asyncio.wait_for(knock_started.wait(), timeout=10.0)
        assert journal.knocks_started == 1, "the flush never reached the knock"
        assert journal.begun == 1 and journal.committed == 1, (
            "the knock started without one committed transaction over the durable group"
        )
        assert journal.appended == 1 and journal.state_upserts == 1
        assert journal.stream_counters == 1
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task
        assert journal.committed == 1 and journal.rolled_back == 0, (
            "the durable record must stand when only the knock is cancelled"
        )
        assert journal.knocks_done == 0, "the cancelled knock did not complete"
        assert emitter.appended == 1, (
            "the durable group committed: the stream-append counter agrees "
            "with it even when the knock is cancelled"
        )
    finally:
        if not flush_task.done():
            flush_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await flush_task
        knock_gate.set()


async def test_flush_and_final_close_commit_one_coherent_record(
    wf_pool: asyncpg.Pool,
    wf_sql: Any,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: Any,
) -> None:
    """The normal path and the final close: append, both upserts, the
    knock, the occurrence count, and the drop count all land as ONE
    coherent record per flush, and aclose's final flush is no
    different."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    node_id = await seed_running_node(wf_conn, wf_schema, flow_id)
    knocks: list[int] = []

    _four_knocks = asyncio.Event()
    listener_conn = await asyncpg.connect(module_pg_schema.pg_dsn)

    def _on_knock(_conn: object, _pid: int, _channel: str, payload: str) -> None:
        # Strict attribution: parse the pointer and keep only THIS
        # test's schema, flow, and node; integer seqs, never raw JSON.
        record = json.loads(payload)
        if (
            record.get("schema") == wf_schema
            and record.get("flow_id") == str(flow_id)
            and record.get("node_id") == str(node_id)
        ):
            knocks.append(int(record["seq"]))
            if len(knocks) >= 4:
                _four_knocks.set()

    await listener_conn.add_listener(PROGRESS_NOTIFY_CHANNEL, _on_knock)
    emitter = ProgressEmitter(
        wf_pool, wf_sql, flow_id=flow_id, node_id=node_id, cadence_s=0, ring_bound=2
    )
    try:
        await emitter.emit(10, "one", None)
        await emitter._flush_once()
        row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert row is not None and int(row["occurrences"]) == 1
        counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
        assert counters is not None
        assert (int(counters["occurrences"]), int(counters["dropped"])) == (1, 0)
        assert await _stream_count(wf_conn, wf_schema, node_id) == 1

        await emitter.emit(20, "two", None)
        await emitter._flush_once()
        row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert row is not None and int(row["occurrences"]) == 2
        assert await _stream_count(wf_conn, wf_schema, node_id) == 2

        await emitter.emit(30, "three", None)
        await emitter._flush_once()
        row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert row is not None and int(row["occurrences"]) == 3
        counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
        assert counters is not None
        assert int(counters["occurrences"]) == 3
        assert int(counters["dropped"]) == emitter.dropped_total, (
            "the drop count on the record must equal the appends' reported trims"
        )
        assert await _stream_count(wf_conn, wf_schema, node_id) == 2

        await emitter.emit(90, "final", {"done": True})
        await emitter.aclose()
        row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert row is not None and int(row["occurrences"]) == 4
        assert row["pct"] == 90 and row["message"] == "final"
        assert await _stream_count(wf_conn, wf_schema, node_id) == 2
        counters = await _progress_row(wf_conn, wf_schema, node_id, STREAM_CHANNEL)
        assert counters is not None and int(counters["occurrences"]) == 4
        assert emitter.appended == emitter.flushes == 4, (
            "one stream-append counter increment per committed flush, never two"
        )

        try:
            await asyncio.wait_for(_four_knocks.wait(), timeout=10.0)
        except TimeoutError:
            raise AssertionError(f"DEBUG knocks={knocks}") from None
        assert len(knocks) == 4, f"one knock per flush expected, got {knocks}"
        assert knocks == sorted(knocks), "the pointers arrive in seq order"
        final_row = await _progress_row(wf_conn, wf_schema, node_id, "progress")
        assert final_row is not None
        assert knocks[-1] == int(final_row["last_seq"]), (
            "the last pointer is the final row's last_seq"
        )
    finally:
        await listener_conn.remove_listener(PROGRESS_NOTIFY_CHANNEL, _on_knock)
        await listener_conn.close()
        await _reap_quietly(emitter)
