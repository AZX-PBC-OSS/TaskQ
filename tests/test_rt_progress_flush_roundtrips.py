"""Red-team pins: the progress flush tick's round-trip shape, and the
mid-await coalescing invariant.

Two contracts, one per test:

* **Round trips per tick (red today — #136).** The flush loop walks
  dirty buffers and awaits one UPDATE per buffer
  (``src/taskq/progress/_flush.py`` — ``for job_id, buffer in ...: await
  _flush_buffer(...)``), so a tick with N dirty jobs costs N sequential
  round trips. Every vendored system that writes per-job state in bulk
  converged on the single-statement multi-row shape: river's columnar
  ``unnest(@id::bigint[], @args::jsonb[])``
  (``vendor/river/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql``),
  graphile-worker's ``delete ... using unnest($1::bigint[])``
  (``vendor/graphile-worker/src/sql/completeJobs.ts``), procrastinate's
  composite-type array (``vendor/procrastinate/procrastinate/sql/queries.sql``).
  The pin asserts the contract in upper-bound form — at most ONE
  statement per flush tick regardless of dirty-buffer count — so a
  batched implementation passes and any better one still passes; only
  the per-buffer sequential shape stays red.

* **Mid-await lost update (green today).** ``_flush_buffer`` snapshots
  the delta before its await and subtracts only the snapshotted portion
  after (``_flush.py`` — snapshot-and-subtract, with per-key survival
  for keys re-written during the await). No existing test forces the
  interleaving: a ``ctx.progress()`` call landing while the flush is
  suspended between snapshot and reset must survive into the next tick.
  This pin suspends the flush inside the mocked statement, mutates the
  buffer exactly as ``progress()`` does, and asserts the flushed
  statement carried the pre-snapshot delta while the new call survived.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._flush import _flush_buffer, progress_flush_loop

_JOB_ID_A = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_JOB_ID_B = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000002")
_WORKER_ID = UUID("11111111-2222-3333-4444-555555555555")


def _dirty_buffer(job_id: UUID, *, base_seq: int = 0, delta: int = 2) -> _ProgressBuffer:
    buf = _ProgressBuffer(job_id=job_id, base_seq=base_seq)
    buf.pending_seq_delta = delta
    buf.pending_state["step"] = 1
    buf.dirty = True
    return buf


def _pool_with_counting_conn(
    *,
    returning_seq: int,
) -> tuple[MagicMock, AsyncMock]:
    conn = AsyncMock()

    async def _fetchrow(*args: object, **kwargs: object) -> dict[str, int]:
        return {"progress_seq": returning_seq}

    conn.fetchrow.side_effect = _fetchrow

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire
    return pool, conn


async def test_flush_tick_costs_at_most_one_statement_regardless_of_dirty_count() -> None:
    """One flush tick with two dirty buffers must reach the backend at
    most once — the single-statement multi-row shape every vendored
    bulk writer converged on (river's unnest arrays, graphile's
    unnest-id delete, procrastinate's composite array). The per-buffer
    sequential await is issue #136: N dirty jobs cost N round trips per
    tick, every tick, for the whole fleet."""
    pool, conn = _pool_with_counting_conn(returning_seq=10)
    buffers: dict[UUID, _ProgressBuffer] = {
        _JOB_ID_A: _dirty_buffer(_JOB_ID_A, base_seq=0, delta=2),
        _JOB_ID_B: _dirty_buffer(_JOB_ID_B, base_seq=5, delta=3),
    }
    assert all(buf.dirty for buf in buffers.values()), "fixture broken: buffers must start dirty"
    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _stop(),
    )

    assert conn.fetchrow.await_count <= 1, (
        f"the flush tick issued {conn.fetchrow.await_count} statements for 2 dirty "
        "buffers — the per-buffer sequential round trip is #136; the contract is "
        "one batched multi-row statement per tick (upper bound: implementations "
        "that do better stay green)"
    )


async def test_progress_landing_mid_await_survives_into_the_next_tick() -> None:
    """A progress() call landing while _flush_buffer is suspended inside
    its statement must survive: the flushed statement carries the
    pre-snapshot delta, and the buffer afterwards holds exactly the
    mid-await call — pending delta 1, the re-written key, still dirty.
    This is the snapshot-and-subtract contract that makes coalescing
    safe under the flush's own await."""
    in_statement = asyncio.Event()
    release = asyncio.Event()
    conn = AsyncMock()

    async def _suspending_fetchrow(*args: object, **kwargs: object) -> dict[str, int]:
        in_statement.set()
        await release.wait()
        return {"progress_seq": 2}

    conn.fetchrow.side_effect = _suspending_fetchrow

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    buf = _dirty_buffer(_JOB_ID_A, base_seq=0, delta=2)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID_A: buf}

    flush_task = asyncio.create_task(
        _flush_buffer(pool, "taskq_test", _JOB_ID_A, _WORKER_ID, buf, buffers)
    )
    await in_statement.wait()

    # Exactly what ctx.progress(step=99) does to the buffer while the
    # flush is suspended between snapshot and reset.
    buf.pending_seq_delta += 1
    buf.pending_state["step"] = 99
    buf.dirty = True

    release.set()
    await flush_task

    assert buf.base_seq == 2, (
        "the flushed statement must carry the pre-snapshot delta (2) and adopt "
        f"the authoritative returned seq; got base_seq={buf.base_seq}"
    )
    assert buf.pending_seq_delta == 1, (
        "only the snapshotted delta may retire — the mid-await call must "
        f"survive; got pending_seq_delta={buf.pending_seq_delta}"
    )
    assert buf.pending_state == {"step": 99}, (
        "a key re-written during the await must survive the snapshot drop — "
        f"got {buf.pending_state!r}"
    )
    assert buf.dirty is True, "a buffer with surviving pending state must stay dirty"
