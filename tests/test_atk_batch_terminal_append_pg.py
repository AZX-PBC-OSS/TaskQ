# ruff: noqa: S608  # Why: schema is a test-fixture identifier, every user value is $-bound.
"""Red-team probe (Postgres): can a TERMINAL batch row gain a new
non-terminal member through a public sequential surface?

The concurrent half of this invariant is already pinned (``test_rt_
queryrace_batch_complete_vs_append.py``: a completion arbitrated while a
member append is uncommitted must delay, the membership lock). This file
attacks the DETERMINISTIC half, no interleave at all: the chunked arms
take an explicit ``batch_id`` and forward it verbatim, and nothing on
those arms consults the batch row's STATUS before writing a member.

Sequence, every step a documented public call:

1. ``enqueue_batch`` with a failure policy creates the batches row and
   its member (the atomic arm).
2. The member is driven terminal through the documented terminal-outcome
   hook, so the batch row flips to ``'complete'``.
3. ``enqueue_batch_streaming(more_items, batch_id=same)`` (the no-extras
   chunked arm) or ``SubJobEnqueuer.enqueue_batch(items, batch_id=same)``
   (the sub-job fallback arm) appends NEW members.

If step 3 lands, a batch row whose status says "every member resolved"
holds pending jobs whose failure_policy counters are dead (every counter
write guards ``status = 'active'``), every ``wait_for_batch`` caller that
already saw ``complete`` has moved on, and the stale-batch sweep (which
only completes ``'active'`` rows) can never reconcile. The extras arms
REFUSE the same reuse with :class:`~taskq.exceptions.BatchIdExistsError`;
the chunked arms must speak the same contract, not silently mutate a
terminal batch.
"""

from dataclasses import replace
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import actor
from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.batch import EnqueueItem, apply_batch_terminal_outcome
from taskq.batch_policy import AbortBatchAfter
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.client._jobs import JobsClient
from taskq.exceptions import BatchIdExistsError
from taskq.testing.fixtures import ModulePgSchema, _open_pg_backend_on_schema

pytestmark = pytest.mark.integration

_QUEUE = "default"
_TERMINAL_LIST = ", ".join(f"'{s}'" for s in sorted(TERMINAL_STATUSES))


class _Payload(BaseModel):
    x: int = 0


@actor(name="atk_batch_terminal_append_actor", queue=_QUEUE)
async def _atk_append_actor(payload: _Payload) -> None:
    pass


async def _seed_actor(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        _atk_append_actor.name,
        _QUEUE,
    )


async def _non_terminal_members(conn: asyncpg.Connection, schema: str, batch_id: UUID) -> int:
    from taskq._json import dumps_str

    val = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        f"WHERE metadata @> $1::jsonb AND status::text NOT IN ({_TERMINAL_LIST})",
        dumps_str({"batch_id": str(batch_id)}),
    )
    assert isinstance(val, int)
    return val


async def _member_count(conn: asyncpg.Connection, schema: str, batch_id: UUID) -> int:
    from taskq._json import dumps_str

    val = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs WHERE metadata @> $1::jsonb',
        dumps_str({"batch_id": str(batch_id)}),
    )
    assert isinstance(val, int)
    return val


async def _complete_batch_row_and_assert_fixture(
    conn: asyncpg.Connection,
    backend: Backend,
    schema: str,
    batch_id: UUID,
    member_job_id: UUID,
) -> None:
    """Drive the batch's single member terminal through the documented
    hook, so the batches row flips to 'complete' the way the terminal
    write would flip it in production."""
    from taskq.backend._protocol import JobRow

    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
        "finished_at = clock_timestamp() WHERE id = $1",
        member_job_id,
    )
    row = await backend.get(member_job_id)
    assert row is not None and isinstance(row, JobRow), "fixture broken: member row vanished"
    assert row.metadata.get("batch_id") == str(batch_id), "fixture broken: member not stamped"
    await apply_batch_terminal_outcome(backend, row, "succeeded")
    batch = await backend.get_batch(batch_id)
    assert batch is not None and batch.status == "complete", (
        "fixture broken: batch did not complete"
    )


async def test_streaming_chunked_arm_refuses_to_append_to_a_complete_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """PUBLIC SURFACE, NO RACE: enqueue_batch (atomic arm) completes, then
    enqueue_batch_streaming reuses the batch_id. The chunked arm must not
    commit pending members under a terminal batch row."""
    stack, _deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        await _seed_actor(clean_pg_conn, module_pg_schema.schema_name)
        client = JobsClient(backend=backend)
        bid = new_uuid()

        first = await client.enqueue_batch(
            [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
            batch_id=bid,
            failure_policy=AbortBatchAfter(5),
        )
        await _complete_batch_row_and_assert_fixture(
            clean_pg_conn, backend, module_pg_schema.schema_name, bid, first.job_handles[0].job_id
        )

        # The attack: the chunked arm, explicit batch_id, no extras, no
        # connection, each chunk its own committed transaction. The
        # terminal batch_id is refused with the same typed error the
        # create_batch arms give a collision.
        with pytest.raises(BatchIdExistsError):
            await client.enqueue_batch_streaming(
                [
                    EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=2)),
                    EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=3)),
                ],
                batch_id=bid,
            )

        non_terminal = await _non_terminal_members(clean_pg_conn, module_pg_schema.schema_name, bid)
        batch = await backend.get_batch(bid)
        assert batch is not None, "fixture broken: batch row vanished"
        assert non_terminal == 0, (
            f"CONTRACT: the refused append must leave the batch exactly as the "
            f"completed call wrote it; found {non_terminal} non-terminal member(s) "
            f"- the refusal must not strand a partial member set."
        )
        assert batch.status == "complete", (
            f"CONTRACT: the refused append must not touch the terminal row; "
            f"got status={batch.status!r}."
        )
        total = await _member_count(clean_pg_conn, module_pg_schema.schema_name, bid)
        assert total == 1, (
            f"CONTRACT: the refused append commits no members (the refused "
            f"chunk is never inserted, the committed prefix is the first "
            f"call's own); found {total} member(s)."
        )
    finally:
        await stack.aclose()


async def test_streaming_chunked_arm_still_appends_to_an_active_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The refusal is terminal-status-only: an ACTIVE batch row keeps
    accepting chunked appends (the resumption semantics the chunked arms
    have always had), so the guard cannot over-refuse a live batch."""
    stack, _deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        await _seed_actor(clean_pg_conn, module_pg_schema.schema_name)
        client = JobsClient(backend=backend)
        bid = new_uuid()

        first = await client.enqueue_batch(
            [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
            batch_id=bid,
            failure_policy=AbortBatchAfter(5),
        )
        assert first.size == 1
        batch = await backend.get_batch(bid)
        assert batch is not None and batch.status == "active", "fixture broken: batch not active"

        await client.enqueue_batch_streaming(
            [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=2))],
            batch_id=bid,
        )
        total = await _member_count(clean_pg_conn, module_pg_schema.schema_name, bid)
        assert total == 2, (
            f"CONTRACT: appending to an ACTIVE batch row must keep working "
            f"(resumption); found {total} member(s), expected 2."
        )
    finally:
        await stack.aclose()


async def test_subjob_enqueuer_fallback_refuses_to_append_to_a_complete_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Same attack through SubJobEnqueuer's no-connection fallback arm:
    per-item backend.enqueue with the batch_id stamp, no membership lock,
    no status check."""
    from taskq._ids import new_uuid

    stack, _deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        await _seed_actor(clean_pg_conn, module_pg_schema.schema_name)
        client = JobsClient(backend=backend)
        bid = new_uuid()

        first = await client.enqueue_batch(
            [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
            batch_id=bid,
            failure_policy=AbortBatchAfter(5),
        )
        await _complete_batch_row_and_assert_fixture(
            clean_pg_conn, backend, module_pg_schema.schema_name, bid, first.job_handles[0].job_id
        )

        enqueuer = SubJobEnqueuer(None, _deps.worker_pool, backend)
        with pytest.raises(BatchIdExistsError):
            await enqueuer.enqueue_batch(
                [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=2))],
                batch_id=bid,
            )

        non_terminal = await _non_terminal_members(clean_pg_conn, module_pg_schema.schema_name, bid)
        batch = await backend.get_batch(bid)
        assert batch is not None, "fixture broken: batch row vanished"
        assert non_terminal == 0, (
            f"CONTRACT: the refused append must leave the terminal batch with "
            f"no non-terminal members; found {non_terminal}."
        )
        total = await _member_count(clean_pg_conn, module_pg_schema.schema_name, bid)
        assert total == 1, (
            f"CONTRACT: the refused fallback append commits no members; found {total} member(s)."
        )
    finally:
        await stack.aclose()


async def test_in_memory_bulk_arm_refuses_to_append_to_a_complete_batch() -> None:
    """Twin pin on the in-memory backend: the bulk preflight refuses a
    terminal batch_id the same way, so the two backends cannot drift on
    which arm mutates a terminal batch."""
    from datetime import UTC, datetime, timedelta

    from taskq.backend._protocol import EnqueueArgs
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    backend = InMemoryBackend(clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))
    client = JobsClient(backend=backend)
    bid = new_uuid()

    first = await client.enqueue_batch(
        [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
        batch_id=bid,
        failure_policy=AbortBatchAfter(5),
    )
    member = backend._jobs[first.job_handles[0].job_id]
    backend._jobs[first.job_handles[0].job_id] = replace(
        member, status="succeeded", finished_at=member.created_at
    )
    await apply_batch_terminal_outcome(
        backend, backend._jobs[first.job_handles[0].job_id], "succeeded"
    )
    batch = await backend.get_batch(bid)
    assert batch is not None and batch.status == "complete", (
        "fixture broken: batch did not complete"
    )

    def _stamped_args(x: int) -> EnqueueArgs:
        return EnqueueArgs(
            id=new_uuid(),
            actor=_atk_append_actor.name,
            queue=_QUEUE,
            payload={"x": x},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime(2025, 1, 1, tzinfo=UTC) - timedelta(seconds=1),
            metadata={"batch_id": str(bid)},
        )

    with pytest.raises(BatchIdExistsError):
        await backend.enqueue_batch([_stamped_args(2), _stamped_args(3)])
    batch_after = await backend.get_batch(bid)
    assert batch_after is not None
    survivors = [
        r
        for r in backend._jobs.values()
        if r.metadata.get("batch_id") == str(bid) and r.status not in TERMINAL_STATUSES
    ]
    assert survivors == [], (
        "CONTRACT: the refused in-memory append must store no members - "
        "the refusal is whole-call, matching the PG bulk arm's single-statement "
        "atomicity."
    )


# ---------------------------------------------------------------------------
# Guard parity: the SINGLE-member write paths.
#
# The bulk arms' guard rides `_lock_batch_membership` (PG) / the bulk
# preflight (in-memory). The sub-job enqueuer's no-connection fallback and
# the in-memory buffer flush write members through the backend's SINGLE
# enqueue instead, and their client-level `get_batch` preflights are
# check-then-act with real await boundaries between the check and each
# write: a batch row that goes terminal mid-call (a concurrent threshold
# abort arbitrating on another connection) must be refused at the WRITE
# SITE, the same typed refusal, or the corruption this file pins comes
# back through the race window the bulk arms already close.
# ---------------------------------------------------------------------------


async def test_single_member_write_refuses_a_terminal_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The single-enqueue write site (the fallback arm's and the flush's
    member path) refuses a terminal batch row: parity with the bulk arms'
    membership lock, which only covers the bulk INSERTs."""
    stack, _deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        await _seed_actor(clean_pg_conn, module_pg_schema.schema_name)
        client = JobsClient(backend=backend)
        bid = new_uuid()

        first = await client.enqueue_batch(
            [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
            batch_id=bid,
            failure_policy=AbortBatchAfter(5),
        )
        await _complete_batch_row_and_assert_fixture(
            clean_pg_conn, backend, module_pg_schema.schema_name, bid, first.job_handles[0].job_id
        )

        from datetime import UTC, datetime, timedelta

        from taskq.backend._protocol import EnqueueArgs

        stamped = EnqueueArgs(
            id=new_uuid(),
            actor=_atk_append_actor.name,
            queue=_QUEUE,
            payload={"x": 2},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime(2026, 1, 1, tzinfo=UTC) - timedelta(seconds=1),
            metadata={"batch_id": str(bid)},
        )
        with pytest.raises(BatchIdExistsError):
            await backend.enqueue(stamped)

        non_terminal = await _non_terminal_members(clean_pg_conn, module_pg_schema.schema_name, bid)
        assert non_terminal == 0, (
            "CONTRACT: the single member write must not land a member under a "
            f"terminal batch row; found {non_terminal} non-terminal member(s)."
        )
        total = await _member_count(clean_pg_conn, module_pg_schema.schema_name, bid)
        assert total == 1, f"the refused single write commits no member; found {total} member(s)."
    finally:
        await stack.aclose()


async def test_single_member_write_still_appends_to_an_active_batch(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The single-write guard is terminal-only: a member write through the
    fallback arm's path into an ACTIVE batch keeps landing (resumption)."""
    stack, _deps, backend = await _open_pg_backend_on_schema(
        module_pg_schema.pg_dsn,
        module_pg_schema.schema_name,
    )
    try:
        await _seed_actor(clean_pg_conn, module_pg_schema.schema_name)
        client = JobsClient(backend=backend)
        bid = new_uuid()

        await client.enqueue_batch(
            [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
            batch_id=bid,
            failure_policy=AbortBatchAfter(5),
        )

        from datetime import UTC, datetime, timedelta

        from taskq.backend._protocol import EnqueueArgs

        stamped = EnqueueArgs(
            id=new_uuid(),
            actor=_atk_append_actor.name,
            queue=_QUEUE,
            payload={"x": 2},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime(2026, 1, 1, tzinfo=UTC) - timedelta(seconds=1),
            metadata={"batch_id": str(bid)},
        )
        await backend.enqueue(stamped)

        total = await _member_count(clean_pg_conn, module_pg_schema.schema_name, bid)
        assert total == 2, (
            f"CONTRACT: an ACTIVE batch keeps accepting member writes "
            f"(resumption); found {total} member(s), expected 2."
        )
    finally:
        await stack.aclose()


async def test_in_memory_single_member_write_refuses_a_terminal_batch() -> None:
    """Twin pin: the in-memory backend's SINGLE enqueue refuses a terminal
    batch_id at the write site, synchronously with the store (no await
    between check and write), so the fallback arm and the buffer flush
    cannot land a member under a terminal row on the twin either."""
    from datetime import UTC, datetime, timedelta

    from taskq.backend._protocol import EnqueueArgs
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    backend = InMemoryBackend(clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))
    client = JobsClient(backend=backend)
    bid = new_uuid()

    first = await client.enqueue_batch(
        [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
        batch_id=bid,
        failure_policy=AbortBatchAfter(5),
    )
    member = backend._jobs[first.job_handles[0].job_id]
    backend._jobs[first.job_handles[0].job_id] = replace(
        member, status="succeeded", finished_at=member.created_at
    )
    await apply_batch_terminal_outcome(
        backend, backend._jobs[first.job_handles[0].job_id], "succeeded"
    )

    stamped = EnqueueArgs(
        id=new_uuid(),
        actor=_atk_append_actor.name,
        queue=_QUEUE,
        payload={"x": 2},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC) - timedelta(seconds=1),
        metadata={"batch_id": str(bid)},
    )
    with pytest.raises(BatchIdExistsError):
        await backend.enqueue(stamped)
    assert stamped.id not in backend._jobs, (
        "CONTRACT: the refused single member write must store nothing."
    )


async def test_flush_buffer_surfaces_terminal_batch_members_through_the_flush_contract() -> None:
    """The buffer flush's refusal speaks the flush arm's OWN contract: a
    per-item failure collected into SubEnqueueError, never a wholesale
    raise. A wholesale raise (a) breaks the documented failure surface
    (``SubEnqueueError`` carries the lost sub-jobs so callers can detect
    them), (b) discards the buffered args with no handle to them, and (c)
    blocks the flush of UNRELATED batches' buffered members. Nothing from
    the terminal batch may land either way - the refusal itself is not in
    dispute, only its shape."""
    from datetime import UTC, datetime, timedelta

    from taskq.backend._protocol import EnqueueArgs
    from taskq.exceptions import SubEnqueueError
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    backend = InMemoryBackend(clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))
    client = JobsClient(backend=backend)

    terminal_bid = new_uuid()
    first = await client.enqueue_batch(
        [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
        batch_id=terminal_bid,
        failure_policy=AbortBatchAfter(5),
    )
    member = backend._jobs[first.job_handles[0].job_id]
    backend._jobs[first.job_handles[0].job_id] = replace(
        member, status="succeeded", finished_at=member.created_at
    )
    await apply_batch_terminal_outcome(
        backend, backend._jobs[first.job_handles[0].job_id], "succeeded"
    )

    active_bid = new_uuid()
    await client.enqueue_batch(
        [EnqueueItem(actor_ref=_atk_append_actor, payload=_Payload(x=1))],
        batch_id=active_bid,
    )

    enqueuer = SubJobEnqueuer(None, None, backend)

    def _stamped(batch_id: UUID, x: int) -> EnqueueArgs:
        return EnqueueArgs(
            id=new_uuid(),
            actor=_atk_append_actor.name,
            queue=_QUEUE,
            payload={"x": x},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime(2025, 1, 1, tzinfo=UTC) - timedelta(seconds=1),
            metadata={"batch_id": str(batch_id)},
        )

    terminal_args = _stamped(terminal_bid, 2)
    active_args = _stamped(active_bid, 3)
    enqueuer._pending_buffer.extend([terminal_args, active_args])

    with pytest.raises(SubEnqueueError) as exc_info:
        await enqueuer.flush_buffer()

    failed_args = [args for args, _ in exc_info.value.failed_items]
    assert failed_args == [terminal_args], (
        "CONTRACT: only the terminal batch's members are reported lost, the "
        "flush arm's per-item failure collection (SubEnqueueError), not a "
        f"wholesale refusal; got {failed_args}."
    )
    assert terminal_args.id not in backend._jobs, (
        "CONTRACT: the terminal batch's buffered member must not land."
    )
    assert active_args.id in backend._jobs, (
        "CONTRACT: an unrelated active batch's buffered member still flushes - "
        "one terminal batch must not block the others' delivery."
    )
    terminal_members = [
        r
        for r in backend._jobs.values()
        if r.metadata.get("batch_id") == str(terminal_bid) and r.status not in TERMINAL_STATUSES
    ]
    assert terminal_members == [], "the terminal batch gained no member."
