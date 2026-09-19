"""Unit tests for the on_cancel hook and the auditability of cancelled attempts.

Cooperative cancellation is a terminal outcome an operator has to be able to
react to and account for after the fact, the same way success and retry
exhaustion are. Two properties are pinned here.

The hook is best-effort in both directions: it must fire, and it must never be
able to break or stall the terminal write it runs beside.

First, the hook surface: an actor must be able to declare an ``on_cancel``
callback alongside ``on_success`` and ``on_retry_exhausted``, and that callback
must be invoked through a best-effort, timeout-bounded helper when the job ends
cancelled. Without it, cleanup that has to happen when work is cut short (
releasing an external reservation, tearing down a remote session, notifying a
caller) has nowhere to hang, and the only signal the operator gets is a row
quietly moving to ``cancelled``.

Second, the audit trail: a cancelled attempt must record why it ended, in the
same places a failed attempt records it. ``mark_failed`` stamps ``error_class``
onto the ``job_attempts`` row and into the ``state_change`` event detail, so a
postmortem can tell one terminal outcome from another by querying history
alone. A cancelled attempt that leaves those fields empty is indistinguishable
from any other cancel, which makes "why did this job stop" unanswerable without
worker logs that may already have rolled off.
"""

from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend._sql_templates import render
from taskq.retry import RetryPolicy

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


class _Payload(BaseModel):
    pass


class _Result(BaseModel):
    ok: bool = True


# ── on_cancel hook surface ──────────────────────────────────────────────


def test_actor_decorator_accepts_on_cancel_hook() -> None:
    """``@actor`` accepts an ``on_cancel`` keyword, mirroring the existing
    ``on_success`` / ``on_retry_exhausted`` hook surface.

    An actor whose work holds external state needs a place to run cleanup when
    it is cancelled mid-flight. If the keyword is not part of the decorator's
    signature, that cleanup cannot be declared at all and decoration raises
    TypeError.
    """
    fired: list[object] = []

    def _on_cancel(job_row: object) -> None:
        fired.append(job_row)

    try:

        @actor(
            name="on_cancel_hook_actor",
            retry=RetryPolicy(max_attempts=1),
            on_cancel=_on_cancel,  # type: ignore[call-arg]
        )
        async def _handler(payload: _Payload) -> _Result:
            return _Result()

    except TypeError as exc:
        pytest.fail(
            "on_cancel is not a supported @actor parameter, so an actor has no "
            f"way to declare cleanup for the cooperative-cancel path: {exc!r}"
        )
    else:
        # Decoration not raising is the pin; the bound actor object is the
        # proof the decorator accepted and returned it.
        assert _handler is not None


async def test_invoke_on_cancel_calls_hook_with_terminal_job_row() -> None:
    """``taskq.retry.invoke_on_cancel`` exists and calls the hook with the
    terminal job row, matching the contract ``invoke_on_success`` uses.

    This is the invocation helper the consumer's ``CancelledError`` handling
    needs alongside its shielded ``mark_cancelled`` write. Without it, an
    actor-supplied ``on_cancel`` could be declared but never fired.
    """
    try:
        from taskq.retry import invoke_on_cancel  # type: ignore[attr-defined]
    except ImportError as exc:
        pytest.fail(
            "taskq.retry.invoke_on_cancel does not exist, so the "
            "cooperative-cancel path can never call an actor-supplied "
            f"on_cancel: {exc!r}"
        )
        return

    from taskq.backend._protocol import JobRow

    fired: list[JobRow] = []

    def _on_cancel(job_row: JobRow) -> None:
        fired.append(job_row)

    job_row = JobRow(
        id="01a0a2a7-4c0e-7141-8fa5-7e348b0f9f01",  # type: ignore[arg-type]
        actor="on_cancel_hook_actor",
        queue="default",
        status="cancelled",
        payload={},
        payload_schema_ver=1,
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        started_at=datetime(2025, 1, 1, tzinfo=UTC),
        finished_at=datetime(2025, 1, 1, tzinfo=UTC),
    )  # type: ignore[call-arg]

    await invoke_on_cancel(_on_cancel, job_row, timeout=3.0)

    assert fired == [job_row], (
        "invoke_on_cancel must call the on_cancel hook with the terminal "
        "(cancelled) job row, the same contract invoke_on_success uses"
    )


async def test_invoke_on_cancel_is_best_effort_and_timeout_bounded() -> None:
    """``invoke_on_cancel`` swallows a raising hook and bounds a hanging one by
    its timeout, so cleanup can never block or break the terminal write.

    The cancel hook runs next to the shielded terminal write that moves the job
    to ``cancelled``. If a buggy hook could raise into that path, or hang on a
    remote call that never answers, the row would be left ``running`` with a
    lease that only the reclaim sweep eventually clears - a cleanup callback
    turning into a stuck job. The hook is therefore best-effort and
    timeout-bounded exactly like ``on_success`` and ``on_retry_exhausted``:
    failures are logged, never propagated, and a slow hook is abandoned.
    """
    import asyncio
    import time

    try:
        from taskq.retry import invoke_on_cancel  # type: ignore[attr-defined]
    except ImportError as exc:
        pytest.fail(
            "taskq.retry.invoke_on_cancel does not exist, so the "
            "cooperative-cancel path can never call an actor-supplied "
            f"on_cancel: {exc!r}"
        )
        return

    from taskq.backend._protocol import JobRow

    job_row = JobRow(
        id="01a0a2a7-4c0e-7141-8fa5-7e348b0f9f02",  # type: ignore[arg-type]
        actor="on_cancel_hook_actor",
        queue="default",
        status="cancelled",
        payload={},
        payload_schema_ver=1,
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        started_at=datetime(2025, 1, 1, tzinfo=UTC),
        finished_at=datetime(2025, 1, 1, tzinfo=UTC),
    )  # type: ignore[call-arg]

    def _raising(_row: JobRow) -> None:
        raise RuntimeError("cleanup blew up")

    await invoke_on_cancel(_raising, job_row, timeout=3.0)

    async def _hanging(_row: JobRow) -> None:
        await asyncio.sleep(30)

    started = time.monotonic()
    await invoke_on_cancel(_hanging, job_row, timeout=0.05)
    elapsed = time.monotonic() - started

    assert elapsed < 5.0, (
        "invoke_on_cancel must bound a hanging on_cancel hook by its timeout so "
        "cleanup never blocks the terminal cancel write; the call took "
        f"{elapsed:.2f}s against a 0.05s timeout"
    )

    await invoke_on_cancel(None, job_row, timeout=3.0)


# ── Cancelled-attempt auditability ──────────────────────────────────────


def _attempt_insert_clause(sql: str) -> str:
    """Return the ``job_attempts`` INSERT ... SELECT portion of a terminal
    write, so the columns it stamps can be inspected without a database."""
    start = sql.index("INSERT INTO", sql.index("job_attempts") - 40)
    return sql[start : sql.index("FROM upd", start)]


def test_cancelled_attempt_records_error_class() -> None:
    """The ``mark_cancelled`` write stamps an ``error_class`` onto the
    ``job_attempts`` row instead of a hardcoded NULL.

    ``mark_failed`` binds ``error_class`` there, which is what lets a
    postmortem query attempt history and tell terminal outcomes apart. A
    cancelled attempt that writes NULL erases the distinction between a
    cooperative cancel, a forced abandon, and an operator-requested stop, and
    the reason survives only in worker logs.
    """
    attempt_clause = _attempt_insert_clause(render("taskq").mark_cancelled)

    assert "'cancelled'," in attempt_clause, (
        "expected the cancelled-attempt INSERT to stamp outcome 'cancelled'; "
        f"the mark_cancelled template changed shape: {attempt_clause!r}"
    )
    assert "NULL, NULL, NULL," not in attempt_clause, (
        "mark_cancelled writes a literal NULL for error_class/error_message/"
        "error_traceback, leaving every cancelled attempt in job_attempts with "
        "no recorded reason. A cancelled attempt must record its cancel reason "
        f"the way a failed attempt records its error class: {attempt_clause!r}"
    )


def test_cancelled_state_change_event_carries_error_class() -> None:
    """The ``state_change`` event emitted by ``mark_cancelled`` includes an
    ``error_class`` in its detail.

    The failed-path event detail carries ``error_class``, so consumers reading
    the event stream can classify terminal transitions without joining back to
    the jobs table. Omitting it on the cancelled path means a stream consumer
    sees only that the job stopped, with no machine-readable reason attached.
    """
    templates = render("taskq")
    cancelled_sql = templates.mark_cancelled
    failed_sql = templates.mark_failed

    failed_detail = failed_sql[failed_sql.index("to_state', 'failed'") :]
    assert "'error_class'" in failed_detail, (
        "baseline assumption broken: the mark_failed state_change detail no "
        "longer carries error_class, so this comparison is meaningless"
    )

    cancelled_detail = cancelled_sql[cancelled_sql.index("to_state', 'cancelled'") :]
    assert "'error_class'" in cancelled_detail, (
        "the cancelled state_change event detail carries only from_state, "
        "to_state and worker_id, unlike the failed path which also carries "
        "error_class. A stream consumer therefore cannot classify why a job "
        f"was cancelled: {cancelled_detail!r}"
    )


# ── Actor-config surface ────────────────────────────────────────────────


def test_actor_config_exposes_on_cancel_and_its_timeout() -> None:
    """The actor-config contract the consumer reads exposes ``on_cancel``
    and ``on_cancel_timeout``, alongside the existing hook pairs.

    The consumer reads hooks off the config protocol, not off the
    decorator, so a hook that only the decorator accepts is a hook that
    never fires. Pairing it with its own timeout keeps the cancel hook
    bounded the same way the success and retry-exhausted hooks are, with
    the same 3-second default an operator already knows.
    """
    from taskq.retry import ActorConfigLike

    assert hasattr(ActorConfigLike, "on_cancel"), (
        "the actor-config contract has no on_cancel member, so the consumer "
        "has nothing to read on the cooperative-cancel path"
    )
    assert hasattr(ActorConfigLike, "on_cancel_timeout"), (
        "on_cancel has no paired timeout, so a slow cleanup hook would be "
        "unbounded - unlike on_success and on_retry_exhausted"
    )

    from taskq.testing.actor import StubActorConfig

    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    assert cfg.on_cancel is None  # type: ignore[attr-defined]
    assert cfg.on_cancel_timeout == 3.0, (  # type: ignore[attr-defined]
        "the cancel hook's default timeout must match the other hooks' 3s "
        "default so operators do not have to learn a second number"
    )


def test_actor_decorator_carries_on_cancel_onto_its_config() -> None:
    """An ``on_cancel`` passed to ``@actor`` reaches the config object the
    consumer reads, rather than being accepted and dropped."""

    def _on_cancel(job_row: object) -> None:  # pragma: no cover - never invoked here
        raise AssertionError("not called in this test")

    @actor(
        name="on_cancel_config_actor",
        retry=RetryPolicy(max_attempts=1),
        on_cancel=_on_cancel,  # type: ignore[call-arg]
        on_cancel_timeout=1.5,  # type: ignore[call-arg]
    )
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    config = _handler.config  # type: ignore[attr-defined]
    assert config.on_cancel is _on_cancel, (
        "@actor accepted on_cancel but did not carry it onto the actor config "
        "the consumer reads, so the hook could never fire"
    )
    assert config.on_cancel_timeout == 1.5


# ── Consumer wiring: the cooperative-cancel path fires the hook ─────────


async def test_on_cancel_fires_when_the_actor_ends_cancelled() -> None:
    """``consume_one_job`` invokes ``on_cancel`` with the job row when the
    actor's work ends in cancellation.

    This is the whole point of the hook: work cut short mid-flight usually
    holds something that has to be released - an external reservation, a
    remote session, a caller waiting on a callback. Without the
    invocation, an actor can declare cleanup that silently never runs, and
    the only trace of the cancelled job is a row quietly going terminal.
    """
    import asyncio

    import taskq.obs as obs_mod
    from taskq.context import JobContext
    from taskq.testing.actor import EmptyPayload, FakeBackend, StubActorConfig, as_backend
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._consumer import consume_one_job

    calls: list[object] = []

    def hook(job_row: object) -> None:
        calls.append(job_row)

    async def cancelling_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    backend = FakeBackend()
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_cancel=hook,  # type: ignore[call-arg]
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    with pytest.raises(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=cancelling_actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
        )

    assert calls == [job], (
        "the cooperative-cancel path completed its terminal write without "
        "invoking the actor's on_cancel hook, so declared cleanup never ran"
    )


async def test_on_cancel_is_not_called_when_the_job_succeeds() -> None:
    """A job that runs to completion never fires ``on_cancel``.

    A cleanup hook that fires on the happy path would release resources the
    finished work still owns - the failure mode is worse than not having
    the hook at all.
    """
    import taskq.obs as obs_mod
    from taskq.context import JobContext
    from taskq.testing.actor import EmptyPayload, FakeBackend, StubActorConfig, as_backend
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._consumer import consume_one_job

    calls: list[object] = []

    def hook(job_row: object) -> None:
        calls.append(job_row)

    async def ok_actor(_job: object, _ctx: JobContext[BaseModel]) -> dict[str, object]:
        return {"value": 1}

    backend = FakeBackend()
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_cancel=hook,  # type: ignore[call-arg]
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    result = await consume_one_job(
        as_backend(backend),
        job,
        _WORKER_ID,
        run_actor=ok_actor,
        actor_config=cfg,
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
    )

    assert result == "succeeded"
    assert calls == [], "on_cancel fired for a job that succeeded"


async def test_a_raising_on_cancel_hook_does_not_break_the_terminal_write() -> None:
    """A buggy ``on_cancel`` cannot stop the job reaching ``cancelled``.

    The hook runs beside the shielded terminal write. If a hook's
    exception could escape into that path, a cleanup callback would leave
    the row stuck in ``running`` under a lease only the reclaim sweep
    eventually clears - turning best-effort cleanup into a stuck job.
    """
    import asyncio

    import taskq.obs as obs_mod
    from taskq.context import JobContext
    from taskq.testing.actor import EmptyPayload, FakeBackend, StubActorConfig, as_backend
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._consumer import consume_one_job

    def bad_hook(job_row: object) -> None:
        raise RuntimeError("cleanup blew up")

    async def cancelling_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    backend = FakeBackend()
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_cancel=bad_hook,  # type: ignore[call-arg]
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    with pytest.raises(asyncio.CancelledError):
        await consume_one_job(
            as_backend(backend),
            job,
            _WORKER_ID,
            run_actor=cancelling_actor,
            actor_config=cfg,
            payload_type=EmptyPayload,
            clock=FakeClock(_NOW),
        )

    assert backend.mark_cancelled_calls, (
        "a raising on_cancel hook prevented the terminal cancel write; the "
        "hook must be best-effort and never block the row going terminal"
    )


async def test_a_hanging_on_cancel_hook_is_bounded_by_its_timeout_end_to_end() -> None:
    """A slow ``on_cancel`` cannot stall ``consume_one_job`` past its own timeout.

    ``test_invoke_on_cancel_is_best_effort_and_timeout_bounded`` pins the
    timeout bound at the ``invoke_on_cancel`` unit level directly; this
    pins the same property through the real end-to-end path a worker
    actually takes (``consume_one_job``), the same way
    ``test_a_raising_on_cancel_hook_does_not_break_the_terminal_write``
    pins the raising case end-to-end rather than only at the unit level.
    A hook that never returns must still let the shielded terminal write
    land and ``consume_one_job`` return within ``on_cancel_timeout`` plus
    a small margin - never hang forever waiting on cleanup.
    """
    import asyncio
    import time

    import taskq.obs as obs_mod
    from taskq.context import JobContext
    from taskq.testing.actor import EmptyPayload, FakeBackend, StubActorConfig, as_backend
    from taskq.testing.clock import FakeClock
    from taskq.testing.jobs import make_job_row
    from taskq.worker._consumer import consume_one_job

    async def hanging_hook(job_row: object) -> None:
        # Never resolves on its own; invoke_on_cancel's asyncio.wait_for
        # must be the thing that cuts this off, not cooperative return.
        await asyncio.sleep(3600)

    async def cancelling_actor(_job: object, _ctx: JobContext[BaseModel]) -> object:
        raise asyncio.CancelledError

    backend = FakeBackend()
    cfg = StubActorConfig(
        retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
        on_cancel=hanging_hook,  # type: ignore[call-arg]
        on_cancel_timeout=0.05,  # type: ignore[call-arg]
    )
    job = make_job_row()
    obs_mod.set_otel_enabled(False)

    start = time.monotonic()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(
            consume_one_job(
                as_backend(backend),
                job,
                _WORKER_ID,
                run_actor=cancelling_actor,
                actor_config=cfg,
                payload_type=EmptyPayload,
                clock=FakeClock(_NOW),
            ),
            timeout=5.0,
        )
    elapsed = time.monotonic() - start

    assert elapsed < 5.0, (
        "consume_one_job did not return within the outer 5s guard; a "
        "hanging on_cancel hook stalled the terminal write past its "
        "configured on_cancel_timeout"
    )
    assert backend.mark_cancelled_calls, (
        "a hanging on_cancel hook prevented the terminal cancel write; the "
        "hook must be best-effort and never block the row going terminal"
    )


# ── Documented boundary: the hook cannot fire for cancel-while-pending ──


def test_docs_state_the_hook_cannot_fire_for_a_job_cancelled_before_it_runs() -> None:
    """The actor guide documents that ``on_cancel`` does not fire for a job
    cancelled while still pending or scheduled.

    That job never enters a worker, so no hook of any kind can run for it;
    bookkeeping on that path stays the caller's job. Leaving the boundary
    undocumented invites exactly the bug the hook is meant to prevent - an
    operator relying on cleanup that structurally cannot happen for the
    most common cancel of all, the one an operator issues on a queued job.
    """
    from pathlib import Path

    guide = Path(__file__).resolve().parent.parent / "docs" / "guides" / "actors.md"
    text = guide.read_text(encoding="utf-8")

    assert "on_cancel" in text, (
        "the actors guide never mentions on_cancel, so neither the hook nor "
        "its boundary is documented anywhere an actor author would look"
    )

    lowered = text.lower()
    boundary_terms = ("pending", "scheduled", "never reached a worker", "never runs")
    assert any(term in lowered for term in boundary_terms), (
        "the actors guide documents on_cancel without stating that it cannot "
        "fire for a job cancelled before it ever reached a worker"
    )
