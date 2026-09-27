"""Public-API ergonomics: pins where the surface's contract used to mislead.

Every test here exists because a real user reading a docstring (or an error
message) was promised something the code did not deliver, or was handed an
error that did not name the mistake and the remedy. Each test's docstring
names the confusion it prevents. Docstring-truth fixes live in
``src/taskq/client/_taskq.py`` / ``_handle.py`` / ``_jobs.py``; the pins
below hold those fixes in place.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import TypeAdapter

import taskq
from taskq.backend._protocol import Backend, JobId, JobRow, JobStatus
from taskq.client._handle import JobHandle
from taskq.progress import ProgressEvent
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

if TYPE_CHECKING:
    from taskq.settings import TaskQSettings

_RA_NONE = TypeAdapter(type(None))

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_SCHEMA_LABEL = "taskq_test"
_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _row(status: JobStatus = "running", progress_seq: int = 0) -> JobRow:
    row = make_job_row(status=status, progress_seq=progress_seq, actor="test_actor")
    return dataclasses.replace(row, id=cast(JobId, _JOB_ID))


def _load_settings() -> TaskQSettings:
    from taskq.settings import TaskQSettings

    return TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})


def _stub_backend(rows: list[JobRow]) -> Backend:
    """A Backend whose ``get`` pops from *rows* (None once exhausted)."""
    remaining = list(rows)
    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:
        if remaining:
            return remaining.pop(0)
        return None

    backend.get = _get
    return backend


def _silent_pubsub() -> MagicMock:
    """A redis client whose pub/sub never delivers a message.

    This is what a real broker looks like after a job's events have
    already been published (or before any exist): ``get_message`` blocks
    out its timeout, then returns None. Forever.
    """
    pubsub = AsyncMock()

    async def _get_message(
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109
    ) -> None:
        # Yield to the loop the way a real socket read does; a bare
        # ``return None`` would busy-spin the loop and starve the very
        # timers that bound this test.
        await asyncio.sleep(0.05)
        return None

    pubsub.get_message = _get_message
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub
    return redis_client


# ── TaskQ constructor: rejected combinations say so at construction ──────
#
# The class docstring used to claim reload_interval was "ignored" for
# dsn=/pool= — the code rejects it with a ValueError. A user who believed
# the docstring either hit an inexplicable crash or, worse, dropped the
# parameter and silently never got rotation. These pins hold the TRUE
# contract: rejected combinations raise, loudly, naming the remedy.


def test_reload_interval_with_dsn_is_rejected_not_ignored() -> None:
    """``reload_interval`` + ``dsn=`` raises: nothing is silently ignored.

    Confusion prevented: the docstring claimed the parameter was "ignored"
    for ``dsn=``; the code raises. A reader who trusted the doc could not
    tell a crash from a supported configuration.
    """
    with pytest.raises(ValueError, match="pool_factory"):
        taskq.TaskQ(dsn="postgresql://u:p@h/db", reload_interval=60.0)


def test_reload_interval_with_caller_pool_is_rejected() -> None:
    """``reload_interval`` + ``pool=`` raises, naming the missing factory."""
    pool = MagicMock(spec=["close"])  # never opened; construction-time check only
    with pytest.raises(ValueError, match="pool_factory"):
        taskq.TaskQ(pool=pool, reload_interval=60.0)  # type: ignore[arg-type]  # Why: a duck-typed stand-in; the constructor rejects the combination before any pool use


def test_pg_provider_without_dsn_names_the_remedy() -> None:
    """``pg_provider`` without ``dsn`` raises, and the message says what to
    pass instead (``pool_factory``) — not just that something was wrong."""

    class _Provider:
        async def get_pg_credential(self) -> None:  # pragma: no cover - never awaited
            return None

    with pytest.raises(ValueError, match="pool_factory"):
        taskq.TaskQ(dsn=None, pg_provider=_Provider())  # type: ignore[arg-type]  # Why: the protocol stub is never called; the constructor must refuse before any credential fetch


def test_constructor_rejects_two_connection_sources() -> None:
    """dsn + pool_factory together raise — the mutual exclusion the docstring
    promises is enforced, not advisory."""

    async def _factory() -> None:  # pragma: no cover - never awaited
        return None

    with pytest.raises(ValueError, match=r"dsn.*pool_factory|pool_factory.*dsn"):
        taskq.TaskQ(dsn="postgresql://u:p@h/db", pool_factory=_factory)  # type: ignore[arg-type]  # Why: the factory's pool type is irrelevant; the constructor rejects the pair first


def test_constructor_rejects_no_connection_source() -> None:
    """No source at all raises with the three valid spellings named."""
    with pytest.raises(ValueError, match=r"dsn.*pool.*pool_factory"):
        taskq.TaskQ()


def test_constructor_rejects_both_redis_knobs() -> None:
    """``redis_url`` + ``redis_client`` raise, not one silently winning."""
    with pytest.raises(ValueError, match=r"redis_url.*redis_client|redis_client.*redis_url"):
        taskq.TaskQ(dsn="postgresql://u:p@h/db", redis_url="redis://b", redis_client=MagicMock())


# ── JobHandle: the client-less contract ─────────────────────────────────


def test_handle_readback_requires_client_and_names_it() -> None:
    """A backend-only handle refuses read-back ops with an error that names
    the missing dependency.

    Confusion prevented: ``wait()`` works on such a handle while
    ``status()``/``refresh()``/``attempts()``/``cancel()`` raise — a user
    who mixed the two paths got a RuntimeError from nowhere; the message
    says which requirement failed and which construction path carries it.
    """
    handle = JobHandle(
        backend=_stub_backend([]),
        row=_row(),
        result_adapter=_RA_NONE,
        was_existing=False,
    )
    for call in (handle.status, handle.refresh, handle.attempts, handle.cancel):
        with pytest.raises(RuntimeError, match="JobsClient"):
            asyncio.run(call())


def test_handle_requires_client_or_backend() -> None:
    """Constructing with neither raises immediately, not on first use."""
    with pytest.raises(ValueError, match="client= or backend="):
        JobHandle(  # type: ignore[call-arg]  # Why: omitting both required sources is the pinned misuse
            row=_row(),
            result_adapter=_RA_NONE,
            was_existing=False,
        )


# ── cancel on a missing job: KeyError, the stdlib idiom ─────────────────


def test_cancel_missing_job_raises_key_error_naming_the_id() -> None:
    """``JobsClient.cancel`` on a nonexistent id raises KeyError carrying the
    id — the documented contract (TaskQ.cancel, JobHandle.cancel both
    delegate here) pinned on the in-memory backend."""
    from taskq.client._jobs import JobsClient

    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)
    try:
        with pytest.raises(KeyError, match=str(_JOB_ID)):
            asyncio.run(client.cancel(cast(JobId, _JOB_ID)))
    finally:
        asyncio.run(client.close())


# ── progress_stream on an already-terminal job (RED-PROVEN FIX) ──────────
#
# The Redis arm of JobHandle.progress_stream() never read the job's current
# row before subscribing: for a job that was ALREADY terminal when the call
# was made, the channel is silent, nothing re-fetched the row, and the
# generator yielded nothing and never returned — a permanent hang, against
# a docstring promising "yields events until a terminal=True event is
# produced". The fix mirrors TaskQ.stream's Redis arm: one initial row read
# (terminal ⇒ the terminal event immediately) plus an on-timeout re-fetch
# that bounds the fetch→subscribe race and a dropped terminal message.


async def test_progress_stream_redis_already_terminal_terminates() -> None:
    """A terminal job yields its terminal event and returns — it does not
    hang forever on a silent channel.

    Confusion prevented: an SSE endpoint that opened ``progress_stream()``
    for an already-finished job (a retry after a reconnect, a slow client)
    never got a terminal event, the connection just hung.
    """
    backend = _stub_backend([_row(status="succeeded", progress_seq=3)])
    handle = JobHandle(
        backend=backend,
        row=_row(status="succeeded", progress_seq=3),
        result_adapter=_RA_NONE,
        was_existing=False,
        _redis_client=_silent_pubsub(),
        _settings=_load_settings(),
    )
    events: list[ProgressEvent] = []
    async with asyncio.timeout(10):
        async for event in handle.progress_stream():
            events.append(event)
    assert len(events) == 1
    assert events[0].terminal is True


async def test_progress_stream_redis_missed_terminal_message_is_bounded() -> None:
    """A terminal write that lands between the initial read and the
    subscription (its pub/sub message never delivered) still ends the
    stream: the on-timeout re-fetch observes it.

    Confusion prevented: pub/sub is fire-and-forget; without the re-fetch
    the stream hung exactly when the job finished fastest.
    """
    # First read (the initial snapshot): running. Second read (the
    # on-timeout re-fetch): terminal — the message was lost.
    backend = _stub_backend(
        [_row(status="running", progress_seq=1), _row(status="succeeded", progress_seq=2)]
    )
    handle = JobHandle(
        backend=backend,
        row=_row(status="running", progress_seq=1),
        result_adapter=_RA_NONE,
        was_existing=False,
        _redis_client=_silent_pubsub(),
        _settings=_load_settings(),
    )
    events: list[ProgressEvent] = []
    async with asyncio.timeout(10):
        async for event in handle.progress_stream():
            events.append(event)
    assert len(events) == 1
    assert events[0].terminal is True
    assert events[0].status == "succeeded"


# ── The import surface: every __all__ name survives `from taskq import X` ─


def test_every_export_imports_via_from_import() -> None:
    """``from taskq import X`` works for all 123 ``__all__`` names.

    Confusion prevented: ``__all__`` is the documented surface; a name
    listed there but not bound on the package (renamed upstream, import
    dropped in a refactor) fails only at a user's import site, deep in
    their application. This pins the whole surface in one place. The
    getattr-based twin lives in test_public_api_exports; this is the
    from-import spelling a user actually writes.
    """
    for name in taskq.__all__:
        assert getattr(taskq, name) is not None, f"taskq.{name} is None"


def test_every_public_export_is_documented() -> None:
    """Every ``__all__`` export carries a docstring.

    Confusion prevented: the docstring-truth audit holds only where a
    docstring exists; an undocumented export's contract is discovered by
    reading source. A new export must arrive documented.
    """
    undocumented = [name for name in taskq.__all__ if not inspect.getdoc(getattr(taskq, name))]
    assert not undocumented, f"Undocumented public exports: {undocumented}"
