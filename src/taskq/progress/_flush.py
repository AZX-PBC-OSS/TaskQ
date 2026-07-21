"""Coalesced periodic progress flush to Postgres."""

import asyncio
from collections.abc import Callable
from uuid import UUID

import asyncpg
import structlog

from taskq._json import dumps_str
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: canonical identifier regex; copying would drift the validation pattern.
)
from taskq.progress._buffer import _ProgressBuffer

__all__ = ["_flush_buffer", "_flush_buffer_immediate", "progress_flush_loop"]

_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.progress._flush")


async def _flush_buffer(
    worker_pool: asyncpg.Pool,
    schema: str,
    job_id: UUID,
    worker_id: UUID,
    buffer: _ProgressBuffer,
    progress_buffers: dict[UUID, _ProgressBuffer],
) -> None:
    """Execute the delta-form UPDATE for one dirty buffer."""
    sql = (
        f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema validated against _IDENT_RE before interpolation.
        "SET progress_state = COALESCE(progress_state, '{}'::jsonb) || $1::jsonb, "
        "    progress_seq   = progress_seq + $2::int "
        "WHERE id = $3::uuid "
        "  AND status = 'running' "
        "  AND locked_by_worker = $4::uuid "
        "RETURNING progress_seq"
    )

    # Snapshot the delta and state we are about to flush *before* awaiting the
    # DB write. A ctx.progress() call landing while this coroutine is
    # suspended mutates `buffer.pending_seq_delta` / `buffer.pending_state`
    # in place; if we blindly reset the buffer to "clean" after the await we
    # would silently discard that late update (lost-update race). Instead we
    # only subtract/remove what we know we actually flushed.
    snapshot_delta = buffer.pending_seq_delta
    snapshot_state = dict(buffer.pending_state)

    try:
        async with worker_pool.acquire() as conn:
            row = await conn.fetchrow(
                sql,
                dumps_str(snapshot_state),
                snapshot_delta,
                job_id,
                worker_id,
            )
    except Exception as exc:
        if isinstance(exc, asyncio.CancelledError):
            raise
        _log.error(
            "progress-flush-error",
            job_id=job_id,
            error=str(exc),
            kind="progress_flush_error",
        )
        return

    if row is None:
        _log.debug("progress-flush-no-row", job_id=job_id)
        # Job no longer running on this worker — idempotency gate fired.
        # Use pop to avoid KeyError if the consumer's finally block already removed it.
        progress_buffers.pop(job_id, None)
        return

    returned_seq: int = row["progress_seq"]
    buffer.base_seq = returned_seq
    # Only retire the portion of the delta we actually flushed — any
    # additional progress() calls that landed during the await remain
    # pending on top of the new base_seq (seq stays monotonic).
    buffer.pending_seq_delta -= snapshot_delta
    # Only drop keys whose value is unchanged since the snapshot — a key
    # re-written during the await (same or different key) must survive so
    # the next flush picks it up.
    for key, snapshotted_value in snapshot_state.items():
        if key in buffer.pending_state and buffer.pending_state[key] == snapshotted_value:
            del buffer.pending_state[key]
    buffer.dirty = buffer.pending_seq_delta != 0 or bool(buffer.pending_state)
    buffer.last_flush_at = asyncio.get_running_loop().time()


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


async def progress_flush_loop(
    pool_getter: Callable[[], asyncpg.Pool],
    schema: str,
    worker_id: UUID,
    progress_buffers: dict[UUID, _ProgressBuffer],
    coalesce_interval: float,
    shutdown: asyncio.Event,
) -> None:
    """Periodic flush loop: runs until shutdown is set, flushing dirty buffers each tick.

    ``pool_getter`` is resolved on every flush rather than captured once,
    so a credential hot-reload (SIGHUP) that swaps the worker pool takes
    effect immediately — a captured pool would be drained and closed
    seconds after the reload, breaking every subsequent flush.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    while not shutdown.is_set():
        await asyncio.sleep(coalesce_interval)

        for job_id, buffer in list(progress_buffers.items()):
            if not buffer.dirty:
                continue
            try:
                await _flush_buffer(
                    pool_getter(), schema, job_id, worker_id, buffer, progress_buffers
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.error(
                    "progress-flush-error",
                    job_id=job_id,
                    error=str(exc),
                    kind="progress_flush_error",
                )
