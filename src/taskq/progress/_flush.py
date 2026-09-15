"""Coalesced periodic progress flush to Postgres."""

import asyncio
from collections.abc import Callable
from typing import Final
from uuid import UUID

import asyncpg
import structlog

from taskq._json import dumps_jsonb_str
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: canonical identifier regex; copying would drift the validation pattern.
)
from taskq.obs import record_progress_flush_failure
from taskq.progress._buffer import _ProgressBuffer
from taskq.worker._watchdog import LoopLiveness

__all__ = ["_flush_buffer", "_flush_buffer_immediate", "_flush_dirty_set", "progress_flush_loop"]

_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.progress._flush")

# Columnar unnest binding order shared by BOTH flush surfaces — the
# tick's batched multi-row statement and the single-row immediate/crash
# statement (which binds one-element arrays). Keeping one list keeps the
# template's parameter positions and the two call sites from drifting.
_FLUSH_UNNEST_BINDING_ORDER: Final[tuple[str, ...]] = (
    "job_ids: list[UUID]",
    "seq_deltas: list[int]",
    "state_docs: list[str]",  # one JSON document per row, dumps_jsonb_str output
    "attempts: list[int]",  # per-row attempt epochs
    "worker_id: UUID",  # the loop-wide owner, the gate's scalar conjunct
)

# The bounded-batch doctrine (the #120 lesson): no flush statement ever
# carries more than this many rows, so every statement is short-lived and
# independently bounded — an all-in-one statement over the whole dirty set
# is the long-running-statement trap (it times out as a whole and stalls
# the tick). 32 rows keeps each UPDATE's lock footprint and runtime tiny
# while still amortising the round trip across many jobs.
_FLUSH_BATCH_ROWS: Final[int] = 32

# The tick's drain cap — at most this many bounded batches per tick, so a
# huge dirty set cannot monopolise the tick; the remainder drains on the
# next tick (the leader sweeps' incremental-commit discipline). 8 x 32 =
# 256 rows per tick at the 0.5 s coalesce cadence.
_FLUSH_MAX_BATCHES_PER_TICK: Final[int] = 8


def _flush_update_sql(schema: str) -> str:
    """Render the single-source-of-truth progress flush UPDATE.

    One statement carries a bounded batch of rows columnar-style — the
    unnest-array bulk-writer shape reduces the number of parameters
    and makes merge operations efficient (array-side operations merge
    per-row without row-by-row iteration), sized by ``_FLUSH_BATCH_ROWS``
    so no statement ever runs long (the #120 doctrine). Each unnest row is
    fenced PER ROW (running + this worker + this attempt epoch) and
    merges PER ROW (monotone base + delta on ``progress_seq``,
    last-writer-wins ``||`` merge on ``progress_state``); a row whose
    fence does not match simply does not update and does not RETURN, so
    it can neither clobber a row it no longer owns nor fail the
    statement. Parameters, in order (see ``_FLUSH_UNNEST_BINDING_ORDER``):

      ``$1::uuid[]``   job ids — unique per statement: the tick flushes
                       dict keys (one buffer per job), and the immediate
                       path binds one row
      ``$2::int[]``    per-row ``progress_seq`` deltas
      ``$3::jsonb[]``  per-row ``progress_state`` merge documents
      ``$4::int[]``    per-row attempt epochs (the fence's epoch conjunct)
      ``$5::uuid``     the flushing worker — the loop-wide owner
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return (
        f'UPDATE "{schema}".jobs AS j '  # noqa: S608  # Why: schema validated against _IDENT_RE immediately above.
        "SET progress_state = COALESCE(j.progress_state, '{}'::jsonb) || f.state_delta, "
        "    progress_seq   = j.progress_seq + f.seq_delta "
        "FROM unnest($1::uuid[], $2::int[], $3::jsonb[], $4::int[]) "
        "    AS f(job_id, seq_delta, state_delta, attempt) "
        "WHERE j.id = f.job_id "
        "  AND j.status = 'running' "
        "  AND j.locked_by_worker = $5::uuid "
        "  AND j.attempt = f.attempt "
        "RETURNING j.id, j.progress_seq"
    )


def _drop_fenced_out_buffer(
    progress_buffers: dict[UUID, _ProgressBuffer],
    job_id: UUID,
    buffer: _ProgressBuffer,
) -> None:
    """Drop a buffer whose row the gate fenced out.

    Identity-checked: a re-dispatch of the same job on this worker seeds
    a NEW buffer (a later attempt epoch) at the same dict key, and the
    stale epoch's no-op must not take the live buffer with it. An
    absent key (the consumer's finally block already removed the
    buffer) is a plain no-op.
    """
    if progress_buffers.get(job_id) is buffer:
        del progress_buffers[job_id]


def _retire_flushed_snapshot(
    buffer: _ProgressBuffer,
    returned_seq: int,
    snapshot_delta: int,
    snapshot_state: dict[str, object],
) -> None:
    """Adopt the authoritative seq and retire ONLY the snapshotted portion.

    A ctx.progress() call landing while the statement was suspended
    mutates ``buffer.pending_seq_delta`` / ``buffer.pending_state`` in
    place; retiring only what was actually flushed preserves that late
    update on top of the new base (seq stays monotone). A key re-written
    during the await (same or different value) survives the drop so the
    next flush picks it up.
    """
    buffer.base_seq = returned_seq
    buffer.pending_seq_delta -= snapshot_delta
    for key, snapshotted_value in snapshot_state.items():
        if key in buffer.pending_state and buffer.pending_state[key] == snapshotted_value:
            del buffer.pending_state[key]
    buffer.dirty = buffer.pending_seq_delta != 0 or bool(buffer.pending_state)
    buffer.last_flush_at = asyncio.get_running_loop().time()


async def _flush_buffer(
    worker_pool: asyncpg.Pool,
    schema: str,
    job_id: UUID,
    worker_id: UUID,
    buffer: _ProgressBuffer,
    progress_buffers: dict[UUID, _ProgressBuffer],
) -> None:
    """Execute the flush UPDATE for one dirty buffer.

    The single-row form of the tick's batched statement: the same
    unnest-columnar template with one-element arrays, so the immediate
    (pre-terminal) and crash-flush paths carry the identical per-row
    fencing gate — running + this worker + this attempt epoch — and the
    identical per-row merge semantics.
    """
    sql = _flush_update_sql(schema)

    # Snapshot the delta and state we are about to flush *before* awaiting
    # the DB write (see _retire_flushed_snapshot for the lost-update
    # contract this preserves).
    snapshot_delta = buffer.pending_seq_delta
    snapshot_state = dict(buffer.pending_state)

    try:
        # The acquire is a POOL-stage operation: a failure or exhaustion
        # here loses every job's flush this tick, not just this one's, so
        # it is labeled with the pool event/kind — the same taxonomy the
        # loop-level pool_getter handler uses — and must not be folded
        # into the per-job handler below. The statement runs in its own
        # try so only its failures count as per-job.
        async with worker_pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    sql,
                    [job_id],
                    [snapshot_delta],
                    [dumps_jsonb_str(snapshot_state)],
                    [buffer.attempt],
                    worker_id,
                )
            except Exception as exc:
                if isinstance(exc, asyncio.CancelledError):
                    raise
                # A failed flush UPDATE loses only this job's progress
                # delta; the pool stages above lose every job's — hence
                # the distinct kinds and stage labels.
                _log.error(
                    "progress-flush-error",
                    job_id=str(job_id),
                    error=str(exc),
                    kind="progress_flush_error",
                )
                record_progress_flush_failure(
                    stage="per_job",
                    error_type=type(exc).__name__,
                )
                return
    except Exception as exc:
        if isinstance(exc, asyncio.CancelledError):
            raise
        # A pool-wide acquire failure/exhaustion loses every job's flush
        # this tick, exactly like the loop-level pool_getter failure —
        # hence the pool event/kind and stage, never the per-job ones.
        _log.error(
            "progress-flush-pool-error",
            job_id=str(job_id),
            error=str(exc),
            kind="progress_flush_pool_error",
        )
        record_progress_flush_failure(
            stage="pool",
            error_type=type(exc).__name__,
        )
        return

    if row is None:
        _log.debug("progress-flush-no-row", job_id=str(job_id))
        # Job no longer running on this worker at this attempt epoch —
        # the idempotency gate fenced the row out.
        _drop_fenced_out_buffer(progress_buffers, job_id, buffer)
        return

    returned_seq: int = row["progress_seq"]
    _retire_flushed_snapshot(buffer, returned_seq, snapshot_delta, snapshot_state)


async def _flush_buffer_immediate(
    worker_pool: asyncpg.Pool,
    schema: str,
    job_id: UUID,
    worker_id: UUID,
    progress_buffers: dict[UUID, _ProgressBuffer],
) -> None:
    """Immediate flush for pre-terminal and crash-flush paths.

    No-op when the buffer does not exist or is not dirty.
    On success, ``buffer.base_seq`` holds the authoritative final seq and
    ``buffer.pending_seq_delta == 0``.
    """
    buffer = progress_buffers.get(job_id)
    if buffer is None or not buffer.dirty:
        return
    await _flush_buffer(worker_pool, schema, job_id, worker_id, buffer, progress_buffers)


async def _flush_dirty_set(
    pool: asyncpg.Pool,
    schema: str,
    worker_id: UUID,
    progress_buffers: dict[UUID, _ProgressBuffer],
    dirty: list[tuple[UUID, _ProgressBuffer]],
) -> None:
    """Flush one tick's dirty set in bounded row-batches.

    The doctrine (the #120 lesson, deliberately re-applied here after a
    one-statement shape regressed it): an all-in-one statement over an
    unbounded dirty set is the long-running-statement trap — it times
    out as a whole, and the timeout kills the tick while the loop
    stalls. The tick therefore drains its dirty set in fixed-size
    row-batches (:data:`_FLUSH_BATCH_ROWS` per statement), each an
    independent short statement on its own connection checkout, with
    :data:`_FLUSH_MAX_BATCHES_PER_TICK` capping the tick so a huge
    dirty set cannot monopolise it — the remainder drains on the next
    tick, the same incremental-commit discipline the leader sweeps
    apply. A failing batch is that batch's failure alone: its buffers
    stay dirty with deltas and pending state intact, and the tick goes
    on flushing the later batches.

    Every batch rides the unnest-columnar template (see
    :func:`_flush_update_sql`) with the fencing gate (running + this
    worker + this attempt epoch) and the progress_seq / progress_state
    merge applied per-row over the unnest arrays. Rows whose gate does
    not match simply do not update and do not RETURN; the retire
    protocol below keys on which job ids came back, so a fenced-out
    row's buffer is dropped exactly as the per-buffer no-op path
    dropped it.
    """
    sql = _flush_update_sql(schema)

    # Snapshot phase — before any await. A ctx.progress() call landing
    # while a batch's statement is suspended mutates the buffer in place;
    # the retire phase subtracts only the snapshotted portion, so the late
    # call survives on top of the new base (snapshot-and-subtract).
    snapshots: list[tuple[UUID, _ProgressBuffer, int, dict[str, object], str]] = []
    for job_id, buffer in dirty:
        snapshot_delta = buffer.pending_seq_delta
        snapshot_state = dict(buffer.pending_state)
        try:
            state_doc = dumps_jsonb_str(snapshot_state)
        except ValueError as exc:
            # The jsonb NUL guard: a permanent data defect in this one
            # row. Skipped and left dirty so the next tick retries (and
            # re-logs) it — the same observable the single-row path has
            # for a poisoned buffer — without poisoning its batch.
            _log.error(
                "progress-flush-error",
                job_id=str(job_id),
                error=str(exc),
                kind="progress_flush_error",
            )
            record_progress_flush_failure(
                stage="per_job",
                error_type=type(exc).__name__,
            )
            continue
        snapshots.append((job_id, buffer, snapshot_delta, snapshot_state, state_doc))

    if not snapshots:
        return

    batches = [
        snapshots[i : i + _FLUSH_BATCH_ROWS] for i in range(0, len(snapshots), _FLUSH_BATCH_ROWS)
    ][:_FLUSH_MAX_BATCHES_PER_TICK]

    for batch in batches:
        batch_job_ids = [snapshot[0] for snapshot in batch]

        try:
            async with pool.acquire() as conn:
                try:
                    rows = await conn.fetch(
                        sql,
                        batch_job_ids,
                        [snapshot[2] for snapshot in batch],
                        [snapshot[4] for snapshot in batch],
                        [snapshot[1].attempt for snapshot in batch],
                        worker_id,
                    )
                except Exception as exc:
                    if isinstance(exc, asyncio.CancelledError):
                        raise
                    # This batch's statement failed: only its jobs lose
                    # their flush this tick — their buffers stay dirty,
                    # deltas intact — and the tick goes on to the later
                    # batches (per-batch isolation: no single unit may
                    # stall the tick). Per-job lines keep the
                    # getter-failure handler's per-job labeling shape;
                    # the stage names the batch as the unit that failed,
                    # distinct from the single-row statement's per_job
                    # stage.
                    for job_id in batch_job_ids:
                        _log.error(
                            "progress-flush-error",
                            job_id=str(job_id),
                            error=str(exc),
                            kind="progress_flush_error",
                        )
                        record_progress_flush_failure(
                            stage="batch",
                            error_type=type(exc).__name__,
                        )
                    continue
        except Exception as exc:
            if isinstance(exc, asyncio.CancelledError):
                raise
            # A pool acquire failure/exhaustion loses this batch's jobs
            # this tick, exactly like the loop-level pool_getter failure
            # — hence the pool event/kind and stage, never the statement
            # ones. Later batches still get their own checkout.
            for job_id in batch_job_ids:
                _log.error(
                    "progress-flush-pool-error",
                    job_id=str(job_id),
                    error=str(exc),
                    kind="progress_flush_pool_error",
                )
                record_progress_flush_failure(
                    stage="pool",
                    error_type=type(exc).__name__,
                )
            continue

        returned_seqs: dict[UUID, int] = {row["id"]: row["progress_seq"] for row in rows}
        for job_id, buffer, snapshot_delta, snapshot_state, _state_doc in batch:
            returned_seq = returned_seqs.get(job_id)
            if returned_seq is None:
                _log.debug("progress-flush-no-row", job_id=str(job_id))
                # The per-row idempotency gate fenced this row out
                # (reclaimed, terminal, or a later attempt epoch owns it).
                _drop_fenced_out_buffer(progress_buffers, job_id, buffer)
                continue
            _retire_flushed_snapshot(buffer, returned_seq, snapshot_delta, snapshot_state)


async def progress_flush_loop(
    pool_getter: Callable[[], asyncpg.Pool],
    schema: str,
    worker_id: UUID,
    progress_buffers: dict[UUID, _ProgressBuffer],
    coalesce_interval: float,
    shutdown: asyncio.Event,
    liveness: LoopLiveness | None = None,
) -> None:
    """Periodic flush loop: runs until shutdown is set, flushing dirty buffers each tick.

    ``pool_getter`` is resolved once per tick rather than captured once,
    so a credential hot-reload (SIGHUP) that swaps the worker pool takes
    effect on the next tick (bounded by ``coalesce_interval``) — a
    captured pool would be drained and closed seconds after the reload,
    breaking every subsequent flush.

    Each tick flushes its whole dirty set as ONE batched multi-row
    statement (see :func:`_flush_dirty_set`), so the tick costs one
    round trip regardless of how many jobs are dirty.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    while not shutdown.is_set():
        await asyncio.sleep(coalesce_interval)

        if liveness is not None:
            liveness.tick("progress_flush", period=coalesce_interval)

        # Snapshot the dirty set before any await: a ctx.progress() call
        # landing mid-tick mutates buffers and must be picked up by the
        # NEXT tick, not raced into this one's in-flight statement.
        dirty = [(job_id, buffer) for job_id, buffer in progress_buffers.items() if buffer.dirty]
        if not dirty:
            continue

        try:
            pool = pool_getter()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The worker cannot obtain a pool at all, so every dirty
            # job's flush this tick is lost — hence the pool event/kind
            # per job, exactly as a getter failure was labeled when the
            # getter was resolved per buffer.
            for job_id, _buffer in dirty:
                _log.error(
                    "progress-flush-pool-error",
                    job_id=str(job_id),
                    error=str(exc),
                    kind="progress_flush_pool_error",
                )
                record_progress_flush_failure(
                    stage="pool",
                    error_type=type(exc).__name__,
                )
            continue

        await _flush_dirty_set(pool, schema, worker_id, progress_buffers, dirty)
