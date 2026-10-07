"""The parent_id stamp: children of a fan-out parent carry the parent's job id.

LIB-2 (issue #670). The parent context already flows as a contextvar
pair: ``set_parent_tags``/``parent_tags`` for tag inheritance
(``client/_enqueuer.py``), set at worker entry (``worker/run.py`` and
``worker/_consumer.py``). The ledger stamp rides the SAME flow as a
sibling contextvar — set together at worker entry, read by every
child-creating enqueue arm, reset with the same finally.

Every enqueue under an ambient parent context stamps ``parent_id``:
``JobsClient.enqueue`` and all three batch arms (regular, streaming,
fast COPY), and the ``SubJobEnqueuer`` arms. No context, no stamp.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel, TypeAdapter

if TYPE_CHECKING:
    from taskq.backend._protocol import JobId

from taskq._ids import new_job_id
from taskq.actor import ActorRef
from taskq.batch import EnqueueItem
from taskq.client._enqueuer import (
    SubJobEnqueuer,
    _parent_job_id_var,
    _parent_tags_var,
    current_parent_id,
    parent_tags,
    set_parent_job_id,
    set_parent_tags,
)
from taskq.client._jobs import JobsClient
from taskq.retry import RetryPolicy
from taskq.testing import FakeClock, InMemoryBackend

if TYPE_CHECKING:
    pass

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(name: str = "child", queue: str = "default") -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue=queue,
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


def _make_client(backend: InMemoryBackend) -> JobsClient:
    return JobsClient(backend, clock=FakeClock(_NOW))


async def test_single_enqueue_stamps_the_ambient_parent() -> None:
    backend = InMemoryBackend(FakeClock(_NOW))
    client = _make_client(backend)
    parent_id = new_job_id()

    token = _parent_job_id_var.set(parent_id)
    try:
        handle = await client.enqueue(_make_actor_ref(), _Payload())
    finally:
        _parent_job_id_var.reset(token)

    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.parent_id == parent_id


async def test_single_enqueue_without_context_stamps_nothing() -> None:
    backend = InMemoryBackend(FakeClock(_NOW))
    client = _make_client(backend)

    handle = await client.enqueue(_make_actor_ref(), _Payload())
    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.parent_id is None


async def test_batch_arm_stamps_every_item() -> None:
    backend = InMemoryBackend(FakeClock(_NOW))
    client = _make_client(backend)
    parent_id = new_job_id()
    items = [EnqueueItem(actor_ref=_make_actor_ref(), payload=_Payload()) for _ in range(3)]

    token = _parent_job_id_var.set(parent_id)
    try:
        batch_handle = await client.enqueue_batch(items)
    finally:
        _parent_job_id_var.reset(token)

    for handle in batch_handle.job_handles:
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.parent_id == parent_id


async def test_batch_streaming_arm_stamps_items() -> None:
    """The streaming generator builds args lazily; the stamp must survive laziness."""
    from collections.abc import Iterator

    backend = InMemoryBackend(FakeClock(_NOW))
    client = _make_client(backend)
    parent_id = new_job_id()

    def _stream() -> Iterator[EnqueueItem[_Payload, _Result]]:
        for _ in range(3):
            yield EnqueueItem(actor_ref=_make_actor_ref(), payload=_Payload())

    token = _parent_job_id_var.set(parent_id)
    try:
        batch_handle = await client.enqueue_batch_streaming(_stream())
    finally:
        _parent_job_id_var.reset(token)

    handles = batch_handle.job_handles
    assert len(handles) == 3
    for handle in handles:
        row = await backend.get(handle.job_id)
        assert row is not None
        assert row.parent_id == parent_id


async def test_sub_job_enqueuer_stamps_the_parent() -> None:
    """The worker-side child-creating path: SubJobEnqueuer under the parent context."""
    backend = InMemoryBackend(FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),
        backend=backend,
        clock=FakeClock(_NOW),
    )
    parent_id = new_job_id()

    token = _parent_job_id_var.set(parent_id)
    try:
        handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
    finally:
        _parent_job_id_var.reset(token)

    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.parent_id == parent_id


async def test_sub_job_enqueuer_without_context_stamps_nothing() -> None:
    backend = InMemoryBackend(FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),
        backend=backend,
        clock=FakeClock(_NOW),
    )
    handle = await enqueuer.enqueue(_make_actor_ref(), _Payload())
    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.parent_id is None


async def test_stamp_is_independent_of_tag_inheritance() -> None:
    """inherit_tags=False suppresses tag inheritance, NOT the ledger stamp.

    The ledger is exact accounting; tags are decoration. A child that
    suppresses its parent's tags is still the parent's child.
    """
    backend = InMemoryBackend(FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),
        backend=backend,
        clock=FakeClock(_NOW),
    )
    parent_id = new_job_id()

    token = _parent_job_id_var.set(parent_id)
    tags_token = set_parent_tags(("t",))
    try:
        handle = await enqueuer.enqueue(_make_actor_ref(), _Payload(), inherit_tags=False)
    finally:
        _parent_tags_var.reset(tags_token)
        _parent_job_id_var.reset(token)

    row = await backend.get(handle.job_id)
    assert row is not None
    assert row.parent_id == parent_id
    assert row.tags == ()


def test_set_parent_job_id_scopes_to_the_task_not_the_worker() -> None:
    """The stamp's contextvar hygiene: default None, set returns a token, reset restores.

    The stale-contextvar attack: a worker entry that forgets its reset
    would leak one job's parent id into every later enqueue on the
    loop. The contract the entry sites (run.py, _consumer.py) rely on:
    the token reset is the ONLY way the context unwinds, and the unset
    default is None.
    """
    assert current_parent_id() is None
    parent_id = new_job_id()
    token = set_parent_job_id(parent_id)
    try:
        assert current_parent_id() == parent_id
    finally:
        _parent_job_id_var.reset(token)
    assert current_parent_id() is None


async def test_concurrent_fan_outs_stamp_in_isolation() -> None:
    """Concurrent fan-outs stamping under contention: the contextvar is
    TASK-scoped, so two parents fanning out on one loop never cross-stamp
    their children (the stale-contextvar attack's concurrency face)."""
    import asyncio

    backend = InMemoryBackend(FakeClock(_NOW))
    client = _make_client(backend)
    parent_a, parent_b = new_job_id(), new_job_id()

    async def fan_out(parent: JobId, n: int) -> list[JobId]:
        token = set_parent_job_id(parent)
        try:
            handles = []
            for _ in range(n):
                handle = await client.enqueue(_make_actor_ref(), _Payload())
                handles.append(handle.job_id)
            return handles
        finally:
            _parent_job_id_var.reset(token)

    ids_a, ids_b = await asyncio.gather(fan_out(parent_a, 5), fan_out(parent_b, 5))

    for job_id in ids_a:
        row = await backend.get(job_id)
        assert row is not None
        assert row.parent_id == parent_a
    for job_id in ids_b:
        row = await backend.get(job_id)
        assert row is not None
        assert row.parent_id == parent_b


def test_parent_tags_cm_sets_and_resets_both_contextvars() -> None:
    """The worker entry's one context manager: tags AND the parent id, reset on all exits."""
    parent_id = new_job_id()

    with pytest.raises(RuntimeError, match="boom"), parent_tags(("t",), job_id=parent_id):
        assert current_parent_id() == parent_id
        raise RuntimeError("boom")

    # Both unwound on the exception path — no stale parent id.
    assert current_parent_id() is None


def test_worker_entry_sites_install_the_parent_id_context() -> None:
    """The stamp only exists if the ENTRY SITES wire it: both worker entry
    points (the real runner and the stub consumer) must install the id
    contextvar beside the tags one, from the claimed job's own id — the
    stale-contextvar attack's first line of defense is the entry's
    guaranteed reset, and a future entry point that forgets the ledger
    half reds here.

    Source-walk pins, the ``test_worker_resolved_capacity_startup_lines``
    pattern: the wiring is a one-line pair inside a long loop, and
    behavioral pins would need the full consumer harness to catch its
    omission.
    """
    runner = Path("src/taskq/worker/run.py").read_text()
    consumer = Path("src/taskq/worker/_consumer.py").read_text()

    assert "parent_tags(tuple(job.tags), job_id=job.id)" in runner
    assert "_parent_job_id_var.set(job.id)" in consumer
    assert "_parent_job_id_var.reset(_parent_job_id_token)" in consumer


async def test_batch_fast_copy_arm_stamps_items() -> None:
    """The COPY path writes parent_id per row (its own column list member)."""
    from taskq.batch import EnqueueItem

    backend = InMemoryBackend(FakeClock(_NOW))
    client = _make_client(backend)
    parent_id = new_job_id()
    items = [EnqueueItem(actor_ref=_make_actor_ref(), payload=_Payload()) for _ in range(3)]

    token = _parent_job_id_var.set(parent_id)
    try:
        await client.enqueue_batch_fast(items)
    finally:
        _parent_job_id_var.reset(token)

    # The fast arm returns a count, not handles; every row it wrote to
    # the (empty) queue is this batch's.
    rows = [r for r in backend._jobs.values() if r.queue == "default"]
    assert len(rows) == 3
    assert all(r.parent_id == parent_id for r in rows)
