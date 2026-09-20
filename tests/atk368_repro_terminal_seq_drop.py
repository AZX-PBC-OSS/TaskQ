"""ATK-368 RED reproducer: the terminal publish drops the consumed seq.

`_run_terminal_path` computes the terminal write's consumed seq
(`_override_seq = head + 1`) and hands it to `_publish_state_change_event`
together with `_override_pending_state`. When the pre-terminal flush
succeeded, the buffer's pending_state is fully retired, so
`_seq_and_state_after_flush_attempt` returns `state=None` — and
`_publish_state_change_event` keys its override path on
`_override_pending_state is not None`, silently DISCARDING the override
seq and re-reading the buffer head. The wire event then repeats the last
progress event's seq: the exact duplicate the branch's contract forbids,
and a seq-cursor consumer discards the terminal event entirely.
"""

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.progress._buffer import (
    _ProgressBuffer,
    _seq_and_state_after_flush_attempt,
)
from taskq.progress._flush import _flush_buffer
from taskq.settings import WorkerSettings

_JOB = UUID("00000000-0000-0000-0000-00000000abcd")
_WORKER = new_uuid()
_NOW = datetime(2026, 1, 1, tzinfo=UTC)


class _RecordingRedis:
    def __init__(self) -> None:
        self.published: list[dict[str, object]] = []

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append(json.loads(payload))
        return 1


class _MergeConn:
    """Applies the real ``row + delta`` merge shape, no yield."""

    def __init__(self, row_seq: list[int]) -> None:
        self._row_seq = row_seq

    async def fetchrow(self, _sql: str, *args: object) -> dict[str, object]:
        deltas = args[1]
        assert isinstance(deltas, list)
        self._row_seq[0] += sum(deltas)
        return {"id": args[0], "progress_seq": self._row_seq[0]}


class _AcquiredConn:
    def __init__(self, conn: _MergeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _MergeConn:
        return self._conn

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _MergePool:
    def __init__(self, row_seq: list[int]) -> None:
        self._row_seq = row_seq

    def acquire(self) -> _AcquiredConn:
        return _AcquiredConn(_MergeConn(self._row_seq))


@pytest.mark.asyncio
async def test_atk_terminal_publish_carries_the_consumed_seq_after_a_landed_flush() -> None:
    """A terminal state-change publish whose caller consumed head+1 must
    put head+1 on the wire — even when the pre-terminal flush retired the
    buffer's pending_state to empty. Today the wire repeats the head."""
    from taskq.progress._publish import _publish_state_change_event

    row_seq = [0]
    pool = _MergePool(row_seq)
    schema = "taskq_atk_repro"
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )
    buffer = _ProgressBuffer(job_id=_JOB, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB: buffer}

    # The attempt's stream: the running transition consumes seq 1, two
    # progress events at 2 and 3.
    buffer.pending_seq_delta += 1  # running, via _consume_state_change_seq
    for _ in range(2):
        buffer.pending_seq_delta += 1
        buffer.dirty = True
        buffer.pending_state["step"] = buffer.pending_seq_delta
        buffer.pending_state["percent"] = 50.0

    # The pre-terminal flush lands: base retires to 3, delta 0, and the
    # pending_state is fully retired (empty).
    await _flush_buffer(pool, schema, _JOB, _WORKER, buffer, buffers)
    assert buffer.base_seq == 3 and buffer.pending_seq_delta == 0
    assert buffer.pending_state == {}

    # Exactly what _run_terminal_path does next.
    seq, state = _seq_and_state_after_flush_attempt(buffer)
    assert seq == 4, "the terminal write must consume one past the head"
    assert state is None, "a fully retired buffer hands the publish no state"

    redis_client = _RecordingRedis()
    await _publish_state_change_event(
        redis_client,
        settings,
        _JOB,
        "some_actor",
        buffers,
        status="succeeded",
        terminal=True,
        _override_seq=seq,
        _override_pending_state=state,
    )

    assert len(redis_client.published) == 1
    wire_seq = redis_client.published[0]["seq"]
    assert wire_seq == 4, (
        f"the terminal wire event must carry the consumed seq 4, got {wire_seq} "
        f"(a repeat of the last progress event's seq 3: a seq-cursor consumer "
        f"discards the terminal event)"
    )
