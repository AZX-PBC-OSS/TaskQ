"""SQL-exception routing pins: the (site x family) cells that used to leak.

Three leak classes this file pins, each against the shared classification
doctrine in ``taskq.worker._transient`` ("any shape a site learns, every
site learns"):

* the notify listener's hand-rolled tuples learned the conn-loss family
  and 57P01 (AdminShutdownError) but not the server-side half of the
  deadline family, 57014 (QueryCanceledError): a DBA ``pg_cancel_backend``
  or a role-level ``statement_timeout`` firing under a server-side stall
  escaped the probe/setup tuples, killed the listener task, and the
  bootstrap TaskGroup read a sibling crash - worker teardown mid-blip;
* the teardown finally's best-effort UNLISTEN suppressed the client-side
  deadline but not the server-side one or raw socket death: a raise in a
  ``finally`` REPLACES the in-flight shutdown exception;
* the 500-lesson: an exception whose own ``__str__``/``__repr__`` raises
  (actor code is the author) converted INSIDE the handlers that were
  classifying it, escaping the per-attempt capture contract and stranding
  the row ``running`` until lease expiry (a false ``WorkerCrashed`` audit
  trail).

The Unprintable exception classes below are the mutation probe: revert any
half of the fixes and the matching pin fails with the leaked exception.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock

import asyncpg
import structlog
from opentelemetry import trace

from taskq._ids import new_uuid
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.worker._handlers import (
    _dispatch_exception,
    _log_terminal_write_failed,
)
from taskq.worker.notify import _health_check_loop, notify_listener_loop

_GRACE = timedelta(seconds=30)
_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()


class _UnprintableStr(Exception):  # noqa: N818  # Why: the name is the pin's mutation probe, an actor-shaped exception class.
    """An exception whose ``__str__`` raises: actor code can ship this."""

    def __str__(self) -> str:
        raise TypeError("__str__ is a lie")


class _UnprintableRepr(Exception):  # noqa: N818  # Why: same probe shape for the repr() arm.
    """An exception whose ``__repr__`` raises: the hook-log shape."""

    def __repr__(self) -> str:
        raise ValueError("__repr__ is a lie")


class _HostileBase(BaseException):
    """A BaseException SUBCLASS: actor code is not limited to Exception-shaped lies."""


class _BaseExceptionShapedStr(Exception):  # noqa: N818  # Why: the name is the pin's mutation probe.
    """An exception whose ``__str__`` raises a BaseException subclass."""

    def __str__(self) -> str:
        raise _HostileBase("__str__ is a BaseException-shaped lie")


class _BaseExceptionShapedRepr(Exception):  # noqa: N818  # Why: the name is the pin's mutation probe.
    """An exception whose ``__repr__`` raises a BaseException subclass."""

    def __repr__(self) -> str:
        raise _HostileBase("__repr__ is a BaseException-shaped lie")


class _MetaNameRaises(type):
    """A metaclass whose ``__name__`` property raises: the safe_repr
    fallback's own interpolation is then uncontrolled input too."""

    @property
    def __name__(cls) -> str:  # type: ignore[reportIncompatibleVariableOverride]  # Why: the raising __name__ IS the probe; a metaclass may shadow type.__name__.
        raise RuntimeError("__name__ is a lie")


class _DoublyHostile(Exception, metaclass=_MetaNameRaises):  # noqa: N818  # Why: same probe shape, every channel hostile at once.
    """Every rendering channel hostile at once: the metaclass ``__name__``
    raises, and so do ``__str__`` and ``__repr__`` -- the ``__repr__``
    raising a BaseException SUBCLASS, the escape shape an ``except
    Exception`` guard lets through."""

    def __str__(self) -> str:
        raise TypeError("__str__ is a lie")

    def __repr__(self) -> str:
        raise _HostileBase("__repr__ is a BaseException-shaped lie")


def _worker_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "pg_dsn": "postgresql://localhost:5432/taskq",
            "schema_name": "taskq_test",
            "notify_health_check_interval": "0.001",
            "notify_listener_setup_timeout": "0.5",
            "notify_reconnect_backoff_initial": "0.01",
        }
    )


def _mock_deps() -> Mock:
    deps = Mock()
    deps.notify_reconnect_lock = (
        asyncio.Lock()
    )  # Why: reconnect serializes on a real lock; a bare Mock fails the async-CM protocol.
    deps.settings = _worker_settings()
    deps.owns_notify_conn = True
    return deps


def _mock_backend() -> PostgresBackend:
    mock_deps = Mock()
    mock_deps.settings.schema_name = "taskq_test"
    mock_deps.worker_pool = Mock()
    return PostgresBackend(
        deps=mock_deps,
        clock=SystemClock(),
        cancellation_grace_period=_GRACE,
        cleanup_grace_period=_GRACE,
    )


def _dead_conn(exc: BaseException) -> Mock:
    conn = Mock()
    conn.execute = AsyncMock(side_effect=exc)
    conn.add_listener = AsyncMock(side_effect=exc)
    # Why raise-at-call: mirrors asyncpg's sync remove_listener on a dead
    # conn, the same shape tc3 uses, so every suppress tuple sees it.
    conn.remove_listener = Mock(side_effect=asyncpg.InterfaceError("conn dead"))
    conn.close = AsyncMock()
    conn.is_closed = Mock(return_value=False)
    conn.terminate = Mock()
    return conn


def _healthy_conn() -> Mock:
    conn = Mock()
    conn.execute = AsyncMock()
    conn.add_listener = AsyncMock()
    conn.remove_listener = AsyncMock()
    conn.close = AsyncMock()
    conn.is_closed = Mock(return_value=False)
    conn.terminate = Mock()
    return conn


# ── Leak 1: the probe's 57014 half ──────────────────────────────────────


async def test_notify_probe_survives_server_side_statement_cancel() -> None:
    """A QueryCanceledError on the health-check probe is routed into the
    reconnect path and the loop keeps serving - it must not kill the task
    the bootstrap TaskGroup is parked on."""
    cancel = asyncpg.QueryCanceledError("canceling statement due to statement timeout")
    dead = _dead_conn(cancel)
    healthy = _healthy_conn()
    deps = _mock_deps()
    deps.notify_conn = dead

    factory_calls = 0

    async def factory() -> Mock:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            raise asyncpg.InterfaceError("first reconnect fails too, the loop must back off")
        return healthy

    deps.notify_conn_factory = factory

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        _health_check_loop(deps, _mock_backend(), shutdown, []),
        name="notify.probe-57014",
    )

    # The reconnect path ran (the probe error was classified, not fatal).
    for _ in range(200):
        await asyncio.sleep(0.005)
        if factory_calls >= 2:
            break
    assert factory_calls >= 2, "the probe's QueryCanceledError never reached the reconnect path"
    assert not task.done(), (
        f"the health-check loop died on QueryCanceledError: {task.exception()!r}"
    )

    shutdown.set()
    await asyncio.wait_for(task, timeout=5.0)
    task_exc = task.exception()
    if task_exc is not None:  # pragma: no cover - wait_for re-raises first
        raise task_exc


async def test_notify_setup_survives_server_side_statement_cancel() -> None:
    """The same 57014 family racing the INITIAL LISTEN setup routes into
    the recovery path instead of escaping the bootstrap TaskGroup."""
    cancel = asyncpg.QueryCanceledError("canceling statement due to statement timeout")
    dead = _dead_conn(cancel)
    dead.add_listener = AsyncMock(side_effect=cancel)
    healthy = _healthy_conn()
    healthy.add_listener = AsyncMock()

    deps = _mock_deps()
    deps.notify_conn = dead

    factory_calls = 0

    async def factory() -> Mock:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            raise asyncpg.InterfaceError("reconnect blip, the retry ladder must absorb it")
        return healthy

    deps.notify_conn_factory = factory

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        notify_listener_loop(deps, _mock_backend(), shutdown, _WORKER_ID),
        name="notify.setup-57014",
    )

    for _ in range(200):
        await asyncio.sleep(0.005)
        if factory_calls >= 2:
            break
    assert factory_calls >= 2, "the setup QueryCanceledError never reached the reconnect path"
    assert not task.done() or task.exception() is None, (
        f"notify_listener_loop died on QueryCanceledError: {task.exception()!r}"
    )

    shutdown.set()
    await asyncio.wait_for(task, timeout=5.0)


async def test_notify_teardown_unlisten_server_cancel_is_suppressed() -> None:
    """The shutdown UNLISTEN's suppress covers the server-side cancel and
    raw socket death: a raise in the teardown finally would REPLACE the
    in-flight shutdown exception."""
    cancel = asyncpg.QueryCanceledError("canceling statement due to statement timeout")
    conn = _healthy_conn()
    conn.remove_listener = Mock(side_effect=cancel)

    deps = _mock_deps()
    deps.notify_conn = conn
    deps.notify_conn_factory = None

    shutdown = asyncio.Event()
    shutdown.set()  # the loop body exits immediately; only the finally runs
    task = asyncio.create_task(
        notify_listener_loop(deps, _mock_backend(), shutdown, _WORKER_ID),
        name="notify.teardown-57014",
    )
    await asyncio.wait_for(task, timeout=5.0)
    task_exc = task.exception()
    if task_exc is not None:  # pragma: no cover - wait_for re-raises first
        raise task_exc


# ── Leak 2: the 500-lesson (unprintable exceptions inside handlers) ─────


async def test_hostile_str_actor_exception_is_captured_not_fatal() -> None:
    """_handle_generic_exception must complete for an exception whose
    __str__ raises: the row records the actor's failure with the constant
    marker, the job does NOT strand running until lease expiry."""
    from dataclasses import replace

    backend = InMemoryBackend(clock=FakeClock(_NOW))
    job = make_job_row(attempt=3, max_attempts=3)
    backend._jobs[job.id] = replace(job, status="running", locked_by_worker=_WORKER_ID)
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))

    outcome = await _dispatch_exception(
        _UnprintableStr("the message can never be seen"),
        backend=backend,
        job=job,
        worker_id=_WORKER_ID,
        actor_config=cfg,
        max_retry_backoff=timedelta(hours=24),
        consumer_span=trace.get_current_span(),
        log=structlog.get_logger("test"),
        progress_buffers=None,
        worker_pool=None,
        settings=None,
        redis_client=None,
    )

    assert outcome == "failed", "the terminal write must land, not strand the job"
    stored = backend._jobs[job.id]
    assert stored.error_class == "_UnprintableStr"
    assert stored.error_message == "<exception str() failed>"


async def test_log_terminal_write_failed_survives_hostile_str() -> None:
    """The terminal-write-failure log renders the ACTOR's exception through
    the guarded path: a raising __str__ must not convert inside this
    handler and escape with a fresh TypeError."""
    _log_terminal_write_failed(
        structlog.get_logger("test"),
        make_job_row(attempt=1, max_attempts=3),
        _UnprintableStr(),
        asyncpg.PostgresConnectionError("conn dropped mid-write"),
    )


def test_redaction_guards_never_raise() -> None:
    """The shared conversion choke points degrade to CPython's own marker
    instead of raising inside a caller's except block."""
    from taskq.obs import render_exception, safe_exception_message, safe_repr

    text = render_exception(_UnprintableStr())
    assert "<exception str() failed>" in text.message

    assert safe_exception_message(_UnprintableStr()) == "<exception str() failed>"

    rendered = safe_repr(_UnprintableRepr())
    assert "repr() failed" in rendered
    assert "_UnprintableRepr" in rendered, "the class name is the one diagnostic that survives"

    # Well-behaved exceptions are untouched by the guard.
    assert safe_exception_message(ValueError("boom")) == "boom"
    assert safe_repr(ValueError("boom")) == repr(ValueError("boom"))


def test_safe_str_survives_a_base_exception_shaped_str() -> None:
    """A ``__str__`` raising a BaseException SUBCLASS must degrade to the
    fallback marker, not convert inside the guard into an uncaught
    KeyboardInterrupt-shaped escape (CPython's ``traceback._safe_string``
    idiom catches BaseException for exactly this reason)."""
    from taskq.obs import safe_str

    assert safe_str(_BaseExceptionShapedStr()) == "<exception str() failed>"


def test_safe_repr_survives_a_base_exception_shaped_repr() -> None:
    """A ``__repr__`` raising a BaseException SUBCLASS must degrade to the
    fallback marker, not convert inside the guard into an uncaught
    KeyboardInterrupt-shaped escape. :func:`safe_repr` is the same defect
    shape as :func:`safe_str`, one level deeper, so the guard catches
    BaseException for the same reason: a pure string render, never an
    await point."""
    from taskq.obs import safe_repr

    rendered = "<no call>"
    try:
        rendered = safe_repr(_BaseExceptionShapedRepr())
    # Why: the pin asserts NO escape; any escape is captured and asserted on as text.
    except BaseException as trapped:
        rendered = f"ESCAPED: {type(trapped).__name__}"
    assert "repr() failed" in rendered, rendered


def test_doubly_hostile_exception_degrades_through_both_guards() -> None:
    """The every-channel-hostile shape: a metaclass ``__name__`` that raises
    (making safe_repr's fallback interpolation itself a live raise) plus a
    hostile ``__str__`` and a hostile ``__repr__`` raising a BaseException
    SUBCLASS. Both guards return their fallback string; neither may raise
    anything. The capture keeps a live escape out of pytest's own renderer,
    which cannot saferepr this shape (its reporter would INTERNALERROR
    instead of reporting)."""
    from taskq.obs import safe_repr, safe_str

    exc = _DoublyHostile()
    assert safe_str(exc) == "<exception str() failed>"

    outcome = "<no call>"
    try:
        outcome = safe_repr(exc)
    # Why: the pin asserts NO escape; any escape is captured and asserted on as text.
    except BaseException as trapped:
        outcome = f"ESCAPED: {type(trapped).__name__}"
    assert "repr() failed" in outcome, outcome
    assert "<unknown>" in outcome, "the raising metaclass __name__ degrades to the constant"


async def test_retry_hook_failure_log_survives_hostile_repr() -> None:
    """_invoke_hook's contract is 'a raising hook never propagates'; a hook
    exception whose __repr__ ALSO raises must not convert inside the
    swallow and escape the contract."""
    from taskq.retry import _invoke_hook

    async def hostile_hook() -> None:
        raise _UnprintableRepr()

    await _invoke_hook(
        hostile_hook,
        make_job_row(attempt=1, max_attempts=3),
        timeout=1.0,
        name="on_retry_exhausted",
        log=structlog.get_logger("test"),
    )
