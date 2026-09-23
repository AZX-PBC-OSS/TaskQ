"""Pins for the issue-461 class: bare-key lookups on per-attempt/shared maps.

A map whose key is shared across generations (job id, attempt epoch,
backend pid) must never let one generation's bare-key remove/read touch a
DIFFERENT generation's entry: the stale-exit-kills-live bug. Each pin
constructs the interleaving where the map changed hands between install
and remove, and asserts the remover's exit is identity-scoped (the
remover holds the exact entry it installed, the fence
``_drop_fenced_out_buffer`` and ``ActiveJobRegistry.deregister`` apply).
"""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id
from taskq.context import JobContext
from taskq.progress._buffer import _ProgressBuffer
from taskq.testing.actor import (
    EmptyPayload,
    FakeBackend,
    as_backend,
    default_actor_config,
)
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_job_row
from taskq.testing.settings import make_integration_settings
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = UUID(int=0xABCDEF)


def _progress_deps(buffers: dict[UUID, _ProgressBuffer]) -> Any:
    """A WorkerDeps whose progress_buffers is the caller's dict.

    Pools are None and no Redis client is wired: the consumer's exit
    paths then take the bare map-maintenance branches, which is exactly
    the surface under attack (no flush round trip to fake). The settings
    are real (the consumer's timeout math reads them; a Mock leaks a Mock
    into ``asyncio.wait_for``'s comparisons).
    """
    deps = WorkerDeps(
        settings=make_integration_settings("postgresql://taskq:taskq@127.0.0.1:1/taskq"),
        dispatcher_pool=Mock(),
        heartbeat_pool=Mock(),
        worker_pool=None,
        notify_conn=None,
        leader_conn=None,
    )
    deps.progress_buffers = buffers
    return deps


# ── the consumer's progress-buffer exits ─────────────────────────────────


async def test_stale_attempt_exit_spares_the_live_attempts_progress_buffer() -> None:
    """A stale attempt's exit must not pop the live attempt's buffer.

    Deterministic interleaving (no timing needed):

    1. Job J attempt 1 starts on this worker; its consumer installs
       buffer B1 at ``_progress_buffers[J]`` and its actor parks.
    2. J's lease lapses; the SAME worker re-claims it (the issue-461
       premise) as attempt 2. Attempt 2's consumer overwrites the key
       with ITS buffer B2, and its actor parks too.
    3. Attempt 1's actor raises (every exit path reaches the consumer's
       ``finally``). The pre-fix exit did a bare ``_progress_buffers.pop(J)``
       there: the LIVE attempt's buffer B2 leaves the map, the flush
       loop's dirty snapshots stop draining it, and the live attempt's
       periodic progress is stranded until its terminal write.

    The exit must remove only the buffer its own attempt installed.
    """
    buffers: dict[UUID, _ProgressBuffer] = {}
    deps = _progress_deps(buffers)
    started1, started2 = asyncio.Event(), asyncio.Event()
    release1, release2 = asyncio.Event(), asyncio.Event()
    observed: dict[str, Any] = {}

    async def actor1(_job: object, _ctx: JobContext[BaseModel]) -> object:
        started1.set()
        await release1.wait()
        raise RuntimeError("stale attempt dies late")

    async def actor2(_job: object, _ctx: JobContext[BaseModel]) -> object:
        # Runs after attempt 2's consumer eagerly installed ITS buffer:
        # the key holds the live generation's buffer now.
        observed["live_buffer"] = buffers[job1.id]
        started2.set()
        await release2.wait()
        return "ok"

    job1 = make_job_row(attempt=1)
    job2 = replace(job1, attempt=2)

    t1 = asyncio.create_task(
        consume_one_job(
            as_backend(FakeBackend()),
            job1,
            _WORKER_ID,
            deps=deps,
            run_actor=actor1,  # type: ignore[arg-type]
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
        )
    )
    await started1.wait()

    t2 = asyncio.create_task(
        consume_one_job(
            as_backend(FakeBackend()),
            job2,
            _WORKER_ID,
            deps=deps,
            run_actor=actor2,  # type: ignore[arg-type]
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
        )
    )
    await started2.wait()
    live_buffer: _ProgressBuffer = observed["live_buffer"]
    assert buffers[job1.id] is live_buffer, "attempt 2's install did not win the key"

    release1.set()
    await t1

    assert buffers.get(job1.id) is live_buffer, (
        "the stale attempt's exit evicted the live attempt's buffer; the flush "
        "loop's dirty snapshots stop draining the live attempt's progress"
    )

    release2.set()
    await t2
    assert job1.id not in buffers, "the live attempt's own exit must still clean its key"


async def test_stale_cancel_exit_spares_the_live_attempts_progress_buffer() -> None:
    """The cancel-delivery exit is identity-scoped too.

    Same interleaving as the generic-path pin, but attempt 1 exits
    through the ``CancelledError`` handler (a cancel delivered to a
    stale attempt unwinding after a re-claim). The handler reads the
    buffer for its terminal seq/state override AND removes it; the
    pre-fix bare pop handed it the LIVE attempt's buffer: the stale
    attempt consumed the live attempt's seq as its terminal override and
    evicted the live buffer besides.
    """
    buffers: dict[UUID, _ProgressBuffer] = {}
    deps = _progress_deps(buffers)
    started1, started2 = asyncio.Event(), asyncio.Event()
    release1, release2 = asyncio.Event(), asyncio.Event()
    observed: dict[str, Any] = {}

    async def actor1(_job: object, _ctx: JobContext[BaseModel]) -> object:
        started1.set()
        await release1.wait()
        raise asyncio.CancelledError

    async def actor2(_job: object, _ctx: JobContext[BaseModel]) -> object:
        observed["live_buffer"] = buffers[job1.id]
        started2.set()
        await release2.wait()
        return "ok"

    job1 = make_job_row(attempt=1)
    job2 = replace(job1, attempt=2)

    t1 = asyncio.create_task(
        consume_one_job(
            as_backend(FakeBackend()),
            job1,
            _WORKER_ID,
            deps=deps,
            run_actor=actor1,  # type: ignore[arg-type]
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
        )
    )
    await started1.wait()

    t2 = asyncio.create_task(
        consume_one_job(
            as_backend(FakeBackend()),
            job2,
            _WORKER_ID,
            deps=deps,
            run_actor=actor2,  # type: ignore[arg-type]
            actor_config=default_actor_config(),
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
        )
    )
    await started2.wait()
    live_buffer: _ProgressBuffer = observed["live_buffer"]

    release1.set()
    with pytest.raises(asyncio.CancelledError):
        await t1

    assert buffers.get(job1.id) is live_buffer, (
        "the stale cancel's exit evicted the live attempt's buffer (and read "
        "the live buffer's head as its own terminal override)"
    )

    release2.set()
    await t2
    assert job1.id not in buffers


# ── the registry's claim-intent resolves ─────────────────────────────────


async def test_stale_generation_resolve_spares_the_live_claim_intent() -> None:
    """A stale generation's ``resolve_claim`` must not erase the live claim's intent.

    Deterministic interleaving:

    1. Loop iteration A takes job J: ``mark_claimed`` parks the intent.
    2. A's attempt hangs; the row is re-claimed on the SAME worker and a
       sibling iteration B takes it: B's ``mark_claimed`` overwrites the
       intent key.
    3. A unwinds and calls ``resolve_claim(J)`` - a bare-id discard in
       the pre-fix code. B's intent is gone, and the next hand-back pass
       (which excludes exactly registered ids and intent ids) re-pends a
       row B is about to execute: concurrent double execution.

    The resolver must drop only the claim token IT holds.
    """
    registry = ActiveJobRegistry()
    job_id = new_job_id()

    token_a = registry.mark_claimed(job_id)
    token_b = registry.mark_claimed(job_id)  # the re-claim's take overwrote the key
    assert token_a is not token_b

    registry.resolve_claim(job_id, token_a)  # the stale generation's exit
    assert job_id in registry.held_ids(), (
        "the stale generation's resolve erased the live claim's intent; the "
        "hand-back passes would re-pend a row the live claim is about to run"
    )

    registry.resolve_claim(job_id, token_b)  # the live claim's own resolve
    assert job_id not in registry.held_ids()


async def test_register_absorbs_the_intent_whatever_generation_it_came_from() -> None:
    """``register`` still absorbs the intent at its key: the intent's
    whole purpose is the pre-registration window, any registration of
    the key covers whatever intent stands."""
    registry = ActiveJobRegistry()
    job_id = new_job_id()

    registry.mark_claimed(job_id)  # token dropped: register's absorb covers it
    task = asyncio.current_task()
    assert task is not None
    await registry.register(job_id, task, _stub_ctx(job_id))
    # The registration now covers the key; the intent map itself is empty
    # (held_ids() still carries the id, via the registration's own entry).
    assert not registry._claim_intents, (  # pyright: ignore[reportPrivateUsage]  # Why: the pin asserts the intent map itself; held_ids() conflates it with the registrations that absorbed them.
        "register did not absorb the claim intent"
    )
    await registry.deregister(job_id, registry.get(job_id))
    assert job_id not in registry.held_ids()


def _stub_ctx(job_id: Any) -> JobContext[BaseModel]:
    from taskq.testing.jobs import make_job_row  # local: keeps the builder import next to its use

    row = make_job_row()
    return JobContext(
        job_id=job_id,
        actor=row.actor,
        queue=row.queue,
        attempt=row.attempt,
        claim_epoch=row.claim_epoch,
        worker_id=_WORKER_ID,
        payload=EmptyPayload(),
        jobs=_FakeEnqueuer(),
        log=__import__("structlog").get_logger("test"),
    )


class _FakeEnqueuer:  # structurally a SubJobEnqueuer stand-in; register never uses it
    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)


# ── the cron commit gate's pid-keyed maps ────────────────────────────────


async def test_cron_commit_gate_hook_retires_only_its_own_sessions_arm() -> None:
    """A dead session's termination hook must not evict a live session's armed emission.

    The gate maps are pid-keyed because a pooled connection cannot be
    weak-referenced, but a pid does not identify a session (issue #292:
    two Postgres servers in one process, or a server restarted under a
    long-lived worker, hand different connections the same pid). The
    pre-fix hook popped the pid bare: a dead session's exit erased a
    live session's armed emission and that tick's telemetry never
    landed. The arm carries the hook identity of the connection that
    armed it, and the retirement is scoped to its own generation.
    """
    import taskq.worker.cron_loop as cron_loop

    pid = 4242
    hook_own, hook_other = 111, 222
    emitted: list[str] = []

    cron_loop._armed_commit_emits.clear()
    cron_loop._confirmed_listening.clear()
    cron_loop._termination_hooked.clear()
    try:
        # The live session (hook_own) armed its tick's emission at pid P.
        cron_loop._armed_commit_emits[pid] = (
            "nonce-live",
            hook_own,
            lambda: emitted.append("live"),
        )

        # The dead session (hook_other, same pid via the other server) retires.
        cron_loop._forget_commit_gate_session(hook_other, pid)

        assert pid in cron_loop._armed_commit_emits, (
            "the dead session's hook evicted the live session's armed emission; "
            "the live tick's telemetry never lands"
        )

        # The live session's own death retires its own arm.
        cron_loop._forget_commit_gate_session(hook_own, pid)
        assert pid not in cron_loop._armed_commit_emits
        assert emitted == []
    finally:
        cron_loop._armed_commit_emits.clear()
        cron_loop._confirmed_listening.clear()
        cron_loop._termination_hooked.clear()


class _FailingNotifyConn:
    """A cron session whose ``add_listener`` always fails (no LISTEN)."""

    def __init__(self, pid: int) -> None:
        self._pid = pid
        termination_listeners: list[object] = []
        self.termination_listeners = termination_listeners

    def get_server_pid(self) -> int:
        return self._pid

    async def add_listener(self, channel: str, callback: object) -> None:
        raise AttributeError("no LISTEN on this session")

    async def remove_listener(self, channel: str, callback: object) -> None:
        return None

    def add_termination_listener(self, callback: object) -> None:
        self.termination_listeners.append(callback)

    async def execute(self, sql: str, *args: object) -> str:
        raise AttributeError("no round trip here")


async def test_cron_commit_gate_arm_failure_spares_a_foreign_sessions_arm() -> None:
    """The arm-failure fallback pops only its own generation's entry.

    A tick whose gate setup fails takes the inline-emit fallback; the
    pre-fix fallback popped the pid bare. With the pid shared by a LIVE
    session on another connection (two servers in one process), the
    fallback erased the live session's armed emission.
    """
    import taskq.worker.cron_loop as cron_loop

    pid = 5150
    emitted: list[str] = []
    conn = _FailingNotifyConn(pid)

    cron_loop._armed_commit_emits.clear()
    cron_loop._confirmed_listening.clear()
    cron_loop._termination_hooked.clear()
    try:
        # A live session (a different connection, same pid) holds the arm.
        cron_loop._armed_commit_emits[pid] = ("nonce-live", 987654, lambda: emitted.append("live"))
        cron_loop._confirmed_listening.add(pid)

        await cron_loop._emit_on_commit(
            conn,  # type: ignore[arg-type]
            lambda: emitted.append("fallback"),
            schema="taskq",
        )

        # The fallback emitted inline (the gate is gone, telemetry stays).
        assert emitted == ["fallback"], emitted
        assert pid in cron_loop._armed_commit_emits, (
            "the arm-failure fallback evicted the live session's armed emission "
            "parked at the same pid"
        )
        assert cron_loop._armed_commit_emits[pid][0] == "nonce-live"
    finally:
        cron_loop._armed_commit_emits.clear()
        cron_loop._confirmed_listening.clear()
        cron_loop._termination_hooked.clear()


# ── the ctx.progress read-side epoch fence ───────────────────────────────


async def test_ctx_progress_fences_a_stale_attempts_epoch() -> None:
    """A stale ctx must not mutate the live attempt's buffer.

    The pre-fix ``ctx.progress`` read the map bare: after a same-worker
    re-claim seeded the LIVE attempt's buffer at the shared key, the
    stale attempt's context (still reachable from an actor's unwinding
    or shielded section) would mix its progress state into the live
    buffer and consume the live attempt's seq.
    """
    from tests._progress_context import make_progress_context

    buffers: dict[UUID, _ProgressBuffer] = {}
    job_id = new_job_id()
    live_buffer = _ProgressBuffer(job_id=job_id, base_seq=40, attempt=2)
    live_buffer.pending_state["step"] = 2
    buffers[job_id] = live_buffer

    stale_ctx = make_progress_context(buffers, job_id, attempt=1)
    await stale_ctx.progress(step=99)

    assert live_buffer.pending_state["step"] == 2, (
        "the stale attempt's ctx.progress mutated the live attempt's buffer"
    )
    assert live_buffer.pending_seq_delta == 0, "the stale call consumed the live attempt's seq"
    assert not live_buffer.dirty

    # The live generation's own ctx flows unchanged.
    live_ctx = make_progress_context(buffers, job_id, attempt=2)
    await live_ctx.progress(step=3)
    assert live_buffer.pending_state["step"] == 3
    assert live_buffer.dirty
    assert live_buffer.pending_seq_delta == 1


# ── the admin SSE semaphore budget's key ─────────────────────────────────


def test_admin_sse_semaphore_budget_is_scoped_to_the_limit() -> None:
    """A mount's semaphore budget is its own, not the first mount's.

    The map is module-global and outlives any one mount; keyed by topic
    alone, the first mount's ``admin_max_sse_connections`` silently
    governed every later mount of the same topic at a different limit
    (the exact failure taskq.web._sse_limit's (key, limit) fix documents).
    """
    import taskq.web.admin.sse as sse

    sse._TOPIC_SEMAPHORES.clear()
    try:
        tight = sse._get_semaphore("jobs", 2)
        loose = sse._get_semaphore("jobs", 50)
        assert loose is not tight, "the first mount's limit governed a later mount"
        assert tight._value == 2
        assert loose._value == 50
        assert sse._get_semaphore("jobs", 2) is tight, "equal limits share one budget"
    finally:
        sse._TOPIC_SEMAPHORES.clear()
