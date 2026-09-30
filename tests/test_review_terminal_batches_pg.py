# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Focused review attacks on the terminal write + batch resolution (Area 7).

Scope: the terminal UPDATE's fencing under reclaim races, the batches-row
arbitration, ``complete_batch``'s tail-gate + the reissue (#589's fresh
contract), batch status transitions, and ``increment_batch_failures``'
threshold semantics. Nothing else.

Hunted interleavings, worked out on paper from ``_batch_sql.py`` first:

1. THE GRANT GAP (RED).  ``complete_batch``'s bounded handshake takes the
   batches row FOR UPDATE inside ``_bounded_batches_row_wait``'s
   transaction and the completion write runs as the NEXT statement. On
   the transactional-caller shape the lock survives to the caller's
   COMMIT, exactly as the docstring promises ("the lock is held to the
   caller's commit"). On the AUTONOMOUS shape (a bare pool connection --
   every reissue, the ``connection=None`` backend path, the consumer
   hook's own shape) the handshake's transaction COMMITS at the end of
   ``_bounded_batches_row_wait``, releasing the row lock BEFORE the
   completion UPDATE runs. An appender that takes the row in that gap
   holds it from its first member INSERT to its commit; the completion
   UPDATE then blocks on the row lock with a snapshot that predates the
   appender's commit, and the post-wait EPQ re-check re-evaluates the
   NOT EXISTS guard against the ORIGINAL snapshot -- the just-committed
   member is invisible -- so the batch flips 'complete' with a pending
   member. The docstring's serialization claim ("an appender arriving
   after the grant serializes behind the completion") is defeated by the
   gap, and the unbounded holder class the bounded wait exists for (the
   streaming append) parks the completion write unbounded.

2. THE DROPPED REISSUE (RED).  ``apply_batch_terminal_outcome`` returns
   whether the caller owes a post-commit re-arbitration, and its
   docstring says "the caller honors it by calling complete_batch
   (batch_id) again". The production caller -- ``worker/dispatch.py``,
   all three hook sites -- DISCARDS the return. Today the flag is always
   False in production only because the hook happens to run after the
   job's transaction has ended; the wiring that would make the #589
   contract true is absent, and any refactor that moves the hook inside
   the transaction (or a future caller that rides one) silently re-opens
   the stuck-batch hole the reissue was built to close.

3. THE COUNTER'S ARITHMETIC (GREEN pins).  N concurrent failing members
   must count exactly N (the bounded-wait skip is the documented M7
   loss class, not a silent race); threshold=1 aborts on the first
   failure and abort wins over complete; a running member outlives the
   abort drain (only pending/scheduled are cancelled) and its hook
   never completes an aborted batch; a completion reissue racing a NEW
   failure converges to exactly one terminal status.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel, ConfigDict

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._batch_sql import (
    complete_batch,
    create_batch,
    get_batch,
    render_batch_sql,
)
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.batch import apply_batch_terminal_outcome
from taskq.context import JobContext
from taskq.testing.fixtures import _open_pg_backend

pytestmark = pytest.mark.integration

_QUEUE = "default"


def _member_args(actor: str, batch_id: UUID) -> EnqueueArgs:
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=_QUEUE,
        payload={"probe": actor},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=60),
        metadata={"batch_id": str(batch_id)},
    )


async def _seed_actor(conn: asyncpg.Connection, schema: str, actor: str) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue) '
        "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
        actor,
        _QUEUE,
    )


async def _non_terminal_members(conn: asyncpg.Connection, schema: str, bid: UUID) -> int:
    val: Any = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        f"WHERE metadata @> $1::jsonb AND status::text NOT IN "
        f"('succeeded', 'failed', 'cancelled', 'crashed', 'abandoned')",
        f'{{"batch_id": "{bid}"}}',
    )
    assert isinstance(val, int)
    return val


async def _teardown(stack: Any, pg_dsn: str, schema: str) -> None:
    await stack.aclose()
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()


async def _seed_terminal_batch(
    deps: Any,
    backend: Any,
    schema: str,
    members: int = 1,
) -> tuple[UUID, list[JobRow]]:
    """A real batch row whose members are ALL terminal (succeeded): the
    state in which the next completion attempt legitimately lands."""
    bid = new_uuid()
    actor = "review_gap_actor"
    async with deps.worker_pool.acquire() as conn:
        await _seed_actor(conn, schema, actor)
        await create_batch(
            conn,
            render_batch_sql(schema),
            bid,
            queue=_QUEUE,
            expected_size=members,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        args = [_member_args(actor, bid) for _ in range(members)]
        rows: list[JobRow] = await backend.enqueue_batch(args, connection=conn)
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
            "finished_at = clock_timestamp() WHERE id = ANY($1::uuid[])",
            [a.id for a in args],
        )
    assert len(rows) == members, "fixture broken: seeding"
    return bid, rows


# ── 1. the grant gap ─────────────────────────────────────────────────


class _GrantGapConn:
    """ConnLike proxy that parks the completion write at a known point.

    When the completion UPDATE (``SET status = 'complete'``) is about to
    run, it signals the appender -- the handshake's grant is held -- and
    waits until the appender has resolved its own membership lock before
    the write executes. The real ``complete_batch`` then runs its
    completion write into that world.
    """

    def __init__(self, inner: Any, gate: _GrantGate) -> None:
        self._inner = inner
        self._gate = gate

    def is_in_transaction(self) -> bool:
        return self._inner.is_in_transaction()

    def transaction(self) -> Any:
        return self._inner.transaction()

    async def execute(self, sql: str, *args: object) -> Any:
        return await self._inner.execute(sql, *args)

    async def fetchval(self, sql: str, *args: object) -> Any:
        return await self._inner.fetchval(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> Any:
        if "SET status = 'complete'" in sql:
            # The completion write is about to run; the handshake's grant
            # is in flight. Let the appender resolve its own row lock
            # first, so the two writers' order is deterministic.
            self._gate.completer_at_write.set()
            await asyncio.wait_for(self._gate.member_held.wait(), timeout=10.0)
        return await self._inner.fetchrow(sql, *args)


class _GrantGate:
    def __init__(self) -> None:
        self.completer_at_write = asyncio.Event()
        self.member_held = asyncio.Event()


async def test_completion_write_races_an_append_into_the_grant_gap(pg_dsn: str) -> None:
    """RED: on the autonomous shape the handshake's lock used to be released
    before the completion write ran, so an appender that took the row in the
    gap committed a member the completion write's snapshot never saw. The
    batch must not reach 'complete' with a non-terminal member -- the
    docstring's "an appender arriving after the grant serializes behind the
    completion" promise, which the gap defeated.

    The choreography is deterministic in both worlds: the proxy parks the
    completion write until the appender has resolved the batches-row lock.
    With the lock and the write in ONE transaction (the fix), the appender's
    bounded FOR UPDATE cannot be granted -- it times out against the
    completer's hold and inserts nothing, the serialization the docstring
    promises. With the two statements split across two transactions (the
    defect), the appender's grant is immediate, its member INSERT rides
    uncommitted under the held lock, and the completion write's snapshot
    plus the post-wait EPQ re-check (original snapshot) complete the batch
    around it."""
    schema = f"rvw_gap_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    batch_sql = render_batch_sql(schema)
    bid, _rows = await _seed_terminal_batch(deps, backend, schema)
    gate = _GrantGate()
    conn_w = await deps.worker_pool.acquire()
    conn_a = await deps.worker_pool.acquire()
    try:

        async def _append() -> str:
            try:
                await asyncio.wait_for(gate.completer_at_write.wait(), timeout=10.0)
                tx_a = conn_a.transaction()
                await tx_a.start()
                try:
                    await conn_a.execute("SET LOCAL lock_timeout = '1s'")
                    try:
                        await conn_a.execute(
                            f'SELECT id FROM "{schema}".batches WHERE id = $1 FOR UPDATE', bid
                        )
                    except asyncpg.exceptions.LockNotAvailableError:
                        # The completer holds the row across its completion
                        # write: the serialization held. Nothing inserted.
                        return "row-held-by-completer"
                    # The row is ours: the streaming-append shape, the
                    # member INSERT uncommitted until this transaction
                    # commits.
                    await backend.enqueue_batch(
                        [_member_args("review_gap_actor", bid)], connection=conn_a
                    )
                    gate.member_held.set()
                    # Hold the lock across the completion write's snapshot,
                    # then commit the member into the completing batch.
                    await asyncio.sleep(0.3)
                    return "appender-took-the-row"
                finally:
                    await tx_a.commit()
            finally:
                gate.member_held.set()

        append_task = asyncio.create_task(_append())

        async def _complete() -> Any:
            return await complete_batch(_GrantGapConn(conn_w, gate), batch_sql, bid)

        arbitrated, outcome = await asyncio.gather(_complete(), append_task)
        assert arbitrated, "fixture broken: the completion attempt did not arbitrate"

        batch: Any = await get_batch(conn_w, batch_sql, bid)
        assert batch is not None, "fixture broken: batch row vanished"
        non_terminal = await _non_terminal_members(conn_w, schema, bid)
        assert outcome == "row-held-by-completer", (
            f"CONTRACT: an appender arriving after the completion grant must "
            f"serialize BEHIND the completion write (the docstring's promise); "
            f"it took the row first: {outcome!r}. The handshake's lock was "
            "released before the completion write ran."
        )
        assert not (batch.status == "complete" and non_terminal > 0), (
            f"CONTRACT: a batch that reached status='complete' must have NO "
            f"non-terminal members. Violated: status={batch.status!r} with "
            f"{non_terminal} non-terminal member(s). The autonomous shape's "
            "handshake released the batches-row lock at the end of "
            "_bounded_batches_row_wait, BEFORE the completion write ran, so "
            "the appender took the row in the gap, its member INSERT was "
            "invisible to the completion write's snapshot, and the post-wait "
            "EPQ re-check re-evaluated the NOT EXISTS guard against the "
            "original snapshot. Every wait_for_batch-style reader that sees "
            "'complete' stops waiting while the member is still pending, and "
            "every counter write (status = 'active' guards) is dead."
        )
    finally:
        await deps.worker_pool.release(conn_w)
        await deps.worker_pool.release(conn_a)
        await _teardown(stack, pg_dsn, schema)


# ── 2. the dropped reissue ───────────────────────────────────────────


class _Payload(BaseModel):
    value: int = 0

    model_config = ConfigDict(extra="forbid")


async def test_dispatch_honors_the_reissue_owed_flag() -> None:
    """RED: the hook's reissue-owed return must survive the caller. The
    recorder stands in for the hook returning True (a gated-out attempt on
    the transactional-caller shape) and the backend records every
    ``complete_batch`` reissue issued WITHOUT a connection -- the
    post-commit re-arbitration. A dispatch that drops the flag leaves the
    recorder empty and turns this pin red; the #589 contract ("the caller
    honors it by calling complete_batch(batch_id) again") then holds for
    the production caller, not just the test suite that reissues by hand."""
    from taskq.backend._protocol import Backend
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.retry import RetryPolicy
    from taskq.testing.actor import FakeBackend, StubActorConfig, as_backend
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from tests.test_dispatch_one_job import (  # Why: the harness lives in the dispatch test module; imported late to keep module import cheap.
        _as_deps,
        _FakeWorkerDeps,
        _make_actor_ref,
        _ScopeStack,
    )

    hook_calls: list[tuple[UUID, str]] = []
    reissues: list[UUID] = []

    async def my_actor(payload: _Payload, ctx: JobContext[_Payload]) -> dict[str, object]:
        return {"ok": True}

    async def _gated_out_hook(
        backend: object,
        job: JobRow,
        outcome: str,
        *,
        transaction_conn: object = None,
    ) -> bool:
        hook_calls.append((job.id, outcome))
        # A gated-out completion attempt on the transactional-caller
        # shape: the caller owes one post-commit re-arbitration.
        return True

    class _ReissueRecorder:
        """Backend proxy recording the connection-less completion reissues."""

        def __init__(self, inner: Backend) -> None:
            self._inner = inner

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

        async def complete_batch(self, batch_id: UUID, *, connection: object = None) -> bool:
            if connection is None:
                reissues.append(batch_id)
            return await self._inner.complete_batch(batch_id, connection=connection)  # type: ignore[union-attr]

    bid = new_uuid()
    fake_backend = FakeBackend()
    fake_deps = _FakeWorkerDeps()
    job = make_job_row(payload={"value": 1})
    job = replace(job, metadata={"batch_id": str(bid)})

    async with _ScopeStack() as scopes:
        recorded = _ReissueRecorder(as_backend(fake_backend))
        with pytest.MonkeyPatch.context() as m:
            m.setattr("taskq.worker.dispatch.apply_batch_terminal_outcome", _gated_out_hook)
            from taskq.worker.dispatch import dispatch_one_job

            await dispatch_one_job(
                backend=recorded,  # type: ignore[arg-type]  # Why: the proxy forwards the full Backend protocol; pyright cannot see through __getattr__.
                deps=_as_deps(fake_deps),
                job=job,
                worker_id=new_uuid(),
                registry=scopes.registry,
                process_scope=scopes.process_scope,
                thread_scope=scopes.thread_scope,
                loop_scope=scopes.loop_scope,
                actor_ref=_make_actor_ref(my_actor),  # type: ignore[arg-type]  # Why: ActorRef[Any, Any] is not ActorRef[BaseModel, BaseModel | None]; pyright cannot widen the generic parameters, but the runtime contract is sound
                actor_config=StubActorConfig(retry=RetryPolicy()),
                clock=FakeClock(datetime.now(UTC)),
                enqueuer=SubJobEnqueuer(
                    backend=as_backend(fake_backend), loop_scope_resolved=None, worker_pool=None
                ),
            )

    assert hook_calls == [(job.id, "succeeded")], (
        f"fixture broken: the hook did not run with the succeeded outcome; got {hook_calls}"
    )
    assert reissues == [bid], (
        f"CONTRACT: a reissue-owed return from the batch hook must be HONORED "
        f"by the production caller -- one post-commit re-arbitration "
        f"complete_batch({bid!s}) with no connection. Got {reissues}. The "
        "#589 contract's caller side (dispatch.py) drops the hook's return, "
        "so a gated-out attempt on the transactional-caller shape leaves the "
        "all-terminal batch 'active' with no attempt left to land -- the "
        "stuck-batch hole the reissue was built to close."
    )


# ── 3. the counter's arithmetic ──────────────────────────────────────


async def test_eight_concurrent_failures_count_exactly_eight(pg_dsn: str) -> None:
    """GREEN pin: 8 concurrent failing members through the REAL hook
    (transactional-caller shape) must count exactly 8. The bounded-wait
    skip is the documented M7 loss class (a >2s batches-row holder), not
    something benign contention may trigger; at this concurrency every
    increment must land."""
    schema = f"rvw_cnt8_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        bid = new_uuid()
        actor = "review_cnt_actor"
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            from taskq.backend._batch_sql import create_batch as _create

            await _create(
                conn,
                render_batch_sql(schema),
                bid,
                queue=_QUEUE,
                expected_size=8,
                failure_threshold=None,
                finalizer_job_id=None,
                originating_actor=None,
            )
            args = [_member_args(actor, bid) for _ in range(8)]
            rows: list[JobRow] = await backend.enqueue_batch(args, connection=conn)
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'running', "
                "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
                "locked_by_worker = gen_random_uuid(), "
                "lock_expires_at = clock_timestamp() + interval '600 seconds' "
                "WHERE id = ANY($1::uuid[])",
                [a.id for a in args],
            )

        async def _fail(row: JobRow) -> None:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'failed', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    row.id,
                )
                await apply_batch_terminal_outcome(backend, row, "failed", transaction_conn=conn)

        await asyncio.wait_for(asyncio.gather(*(_fail(row) for row in rows)), timeout=30.0)
        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.consecutive_failures == 8, (
            f"8 concurrent failing members must count exactly 8 (the bounded-wait "
            f"skip is M7's >2s-holder class, not benign contention); got "
            f"{batch.consecutive_failures}"
        )
    finally:
        await _teardown(stack, pg_dsn, schema)


async def test_threshold_one_first_failure_aborts_and_abort_wins_over_complete(
    pg_dsn: str,
) -> None:
    """GREEN pin: threshold=1 -- the FIRST failure aborts the batch (the
    increment's own count crosses the boundary), pending/scheduled members
    are cancelled, and the succeeding member's completion attempt never
    flips the aborted row back (abort wins, one terminal status)."""
    schema = f"rvw_thr1_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        bid = new_uuid()
        actor = "review_thr_actor"
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            from taskq.backend._batch_sql import create_batch as _create

            await _create(
                conn,
                render_batch_sql(schema),
                bid,
                queue=_QUEUE,
                expected_size=2,
                failure_threshold=1,
                finalizer_job_id=None,
                originating_actor=None,
            )
            args = [_member_args(actor, bid) for _ in range(2)]
            rows: list[JobRow] = await backend.enqueue_batch(args, connection=conn)
            # Only the failing member is running; the sibling stays pending
            # so the abort drain has something to cancel.
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'running', "
                "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
                "locked_by_worker = gen_random_uuid(), "
                "lock_expires_at = clock_timestamp() + interval '600 seconds' "
                "WHERE id = $1",
                rows[0].id,
            )
        fail_row, ok_row = rows

        # The failing member: terminal write + hook in one transaction.
        async with deps.worker_pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'failed', "
                "finished_at = clock_timestamp() WHERE id = $1",
                fail_row.id,
            )
            await apply_batch_terminal_outcome(backend, fail_row, "failed", transaction_conn=conn)

        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.status == "aborted", (
            f"a threshold-1 batch must abort on the FIRST failure (the increment's "
            f"own count crosses the boundary); got {batch.status!r}"
        )
        assert batch.completed_at is not None
        # The abort drain cancelled the still-pending sibling.
        async with deps.worker_pool.acquire() as conn:
            ok_status: Any = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', ok_row.id
            )
        assert ok_status == "cancelled", (
            f"the abort drain must cancel the pending sibling; got {ok_status!r}"
        )
        cancelled_count = await backend.abort_batch(bid)
        assert cancelled_count == 0, "a re-abort must not re-cancel terminal members"

        # The succeeding member (already cancelled by the drain): its hook's
        # completion attempt must never flip the aborted row.
        async with deps.worker_pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                "finished_at = clock_timestamp() WHERE id = $1",
                ok_row.id,
            )
            await apply_batch_terminal_outcome(backend, ok_row, "succeeded", transaction_conn=conn)
        batch = await backend.get_batch(bid)
        assert batch is not None and batch.status == "aborted", (
            f"abort must win: a later succeeded member's completion attempt left "
            f"{batch.status if batch else None!r}"
        )
    finally:
        await _teardown(stack, pg_dsn, schema)


async def test_running_member_outlives_the_abort_and_never_completes(pg_dsn: str) -> None:
    """GREEN pin: the abort drain cancels pending/scheduled members only; a
    RUNNING member's terminal write lands after the flip, its hook's counter
    writes no-op on the aborted row (0, None, 0), and its completion attempt
    never revives or re-flips the batch -- exactly one terminal status."""
    schema = f"rvw_run_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        bid = new_uuid()
        actor = "review_run_actor"
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            from taskq.backend._batch_sql import create_batch as _create

            await _create(
                conn,
                render_batch_sql(schema),
                bid,
                queue=_QUEUE,
                expected_size=2,
                failure_threshold=None,
                finalizer_job_id=None,
                originating_actor=None,
            )
            args = [_member_args(actor, bid) for _ in range(2)]
            rows: list[JobRow] = await backend.enqueue_batch(args, connection=conn)
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'running', "
                "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
                "locked_by_worker = gen_random_uuid(), "
                "lock_expires_at = clock_timestamp() + interval '600 seconds' "
                "WHERE id = ANY($1::uuid[])",
                [a.id for a in args],
            )
        run_row, other_row = rows

        cancelled = await backend.abort_batch(bid)
        assert cancelled == 0, (
            "the abort drain must not cancel RUNNING members (the page predicate "
            f"is pending/scheduled only); cancelled {cancelled}"
        )

        # The running member finishes AFTER the flip: success, hook, and the
        # completion attempt that must no-op on the aborted row.
        async with deps.worker_pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'succeeded', "
                "finished_at = clock_timestamp() WHERE id = $1",
                run_row.id,
            )
            owed = await apply_batch_terminal_outcome(
                backend, run_row, "succeeded", transaction_conn=conn
            )
        batch = await backend.get_batch(bid)
        assert batch is not None and batch.status == "aborted", (
            f"a terminal member's completion attempt must not revive an aborted "
            f"batch; got {batch.status if batch else None!r}"
        )
        assert owed is False, "an autonomous-armed hook on an aborted row owes nothing"

        # The other member fails the same way: the counter stays untouched.
        async with deps.worker_pool.acquire() as conn, conn.transaction():
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'failed', "
                "finished_at = clock_timestamp() WHERE id = $1",
                other_row.id,
            )
            await apply_batch_terminal_outcome(backend, other_row, "failed", transaction_conn=conn)
        batch = await backend.get_batch(bid)
        assert batch is not None
        assert batch.status == "aborted" and batch.consecutive_failures == 0, (
            f"counter writes on a non-active batch must no-op; got status="
            f"{batch.status!r}, consecutive_failures={batch.consecutive_failures}"
        )
    finally:
        await _teardown(stack, pg_dsn, schema)


async def test_completion_reissue_racing_a_new_failure_is_consistent(pg_dsn: str) -> None:
    """GREEN pin: the reissue's post-commit snapshot racing a NEW failure.
    The reissue (no connection) and a new member's failing hook run
    concurrently; the batch converges to exactly one terminal status and
    every count the final state reports is real: either the reissue wins
    (the batch is complete and the late failure's counter write no-ops on
    the non-active row) or the failure wins (the batch stays active and the
    reissue's guard vetoes on the member). No intermediate state survives."""
    schema = f"rvw_reissue_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    batch_sql = render_batch_sql(schema)
    try:
        bid = new_uuid()
        actor = "review_reissue_actor"
        async with deps.worker_pool.acquire() as conn:
            await _seed_actor(conn, schema, actor)
            from taskq.backend._batch_sql import create_batch as _create

            await _create(
                conn,
                batch_sql,
                bid,
                queue=_QUEUE,
                expected_size=1,
                failure_threshold=None,
                finalizer_job_id=None,
                originating_actor=None,
            )
            args = [_member_args(actor, bid)]
            rows: list[JobRow] = await backend.enqueue_batch(args, connection=conn)
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET status = 'running', "
                "started_at = clock_timestamp(), last_heartbeat_at = clock_timestamp(), "
                "locked_by_worker = gen_random_uuid(), "
                "lock_expires_at = clock_timestamp() + interval '600 seconds' "
                "WHERE id = ANY($1::uuid[])",
                [a.id for a in args],
            )
        member = rows[0]

        # The reissue races the member's terminal write + failing hook: the
        # reissue's probe and the hook's increment/abort interleave freely.
        async def _reissue() -> bool:
            return await backend.complete_batch(bid)

        async def _fail_hook() -> None:
            async with deps.worker_pool.acquire() as conn, conn.transaction():
                await conn.execute(
                    f"UPDATE \"{schema}\".jobs SET status = 'failed', "
                    "finished_at = clock_timestamp() WHERE id = $1",
                    member.id,
                )
                await apply_batch_terminal_outcome(backend, member, "failed", transaction_conn=conn)

        await asyncio.wait_for(asyncio.gather(_reissue(), _fail_hook()), timeout=30.0)

        batch = await backend.get_batch(bid)
        assert batch is not None
        non_terminal = await _non_terminal_members_conn(deps, schema, bid)
        if batch.status == "complete":
            # The reissue won: the failure's writes rode the row AFTER the
            # flip, so the counter is untouched and the member is terminal.
            assert non_terminal == 0, (
                f"a 'complete' batch must have no non-terminal members; got {non_terminal}"
            )
        else:
            assert batch.status == "aborted", (
                f"one terminal status must survive the race; got {batch.status!r} "
                f"with {non_terminal} non-terminal member(s)"
            )
    finally:
        await _teardown(stack, pg_dsn, schema)


async def _non_terminal_members_conn(deps: Any, schema: str, bid: UUID) -> int:
    async with deps.worker_pool.acquire() as conn:
        return await _non_terminal_members(conn, schema, bid)
