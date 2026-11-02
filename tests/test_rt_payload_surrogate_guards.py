# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: lone-surrogate strings at the progress and result boundaries.

A lone surrogate (e.g. ``"\\udcff"``, exactly what ``os.fsdecode`` of a
non-UTF-8 filename byte produces) is a legal Python ``str`` that can NEVER
be encoded to UTF-8 -- so it can never reach a PG ``text``/``jsonb`` value.
The NUL guard family (``sanitize_nul_str`` / ``check_no_nul_str`` /
``dumps_jsonb_str``) rejects NUL at every boundary; NOTHING rejects
surrogates. Three defects follow, pinned here in the in-memory tier (the
PG tier twins live in tests/test_rt_payload_pg_jsonb.py):

1. ``ctx.progress(detail=...)`` bypasses the publish-time serialization
   that ``data=`` gets (context.py guards only ``data``), so an
   unencodable detail poisons the coalesce buffer and blows up the
   terminal write's ``jsonb_param`` as an unclassifiable TypeError.
2. An actor result dict containing a lone surrogate fails through the
   generic retryable path (orjson TypeError), burning every attempt --
   the exact burn ResultTooLarge exists to prevent (retry.py: "the actor
   already ran -- a re-run returns the same value").
3. An actor exception whose MESSAGE carries a surrogate stores cleanly
   in the in-memory mirror but strands the job on PG (asyncpg DataError
   is a PostgresError subclass, so the terminal-write classification
   misreads it as transient infra) -- the mirror divergence is pinned
   here as the desired observable; the PG failure is the RED twin.
"""

from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import pytest
import structlog
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.context import JobContext
from taskq.obs import record_exception_text
from taskq.obs._redact_exc import render_exception
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.testing.actor import default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._consumer import consume_one_job
from tests._progress_context import make_progress_context

if TYPE_CHECKING:
    from taskq.backend._protocol import JobRow

# The pre-setup logging chain's module: reloaded by the JSON-rendering pin
# below to reinstall the chain a plain ``import taskq`` produces.
from taskq.obs import (
    _structlog as taskq_obs_structlog,  # pyright: ignore[reportPrivateUsage]  # Why: the pin re-executes the module's own install seam; private prefix scopes it to the package.
)

_SURROGATE = "\udcff"
"""A lone low surrogate: legal Python str, impossible to UTF-8 encode,
and exactly what ``os.fsdecode(b'\\xff')`` yields on a surrogateescape
filesystem."""


class _BufDeps:
    """Duck-typed WorkerDeps carrying only the progress-buffer wiring the
    consumer reads (worker pools/redis left unset so no flush/publish
    path runs -- the terminal write alone must decide the outcome)."""

    def __init__(self, buffers: dict[UUID, _ProgressBuffer]) -> None:
        self.progress_buffers = buffers
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None
        self.settings: WorkerSettings | None = None
        self.redis_client: object | None = None
        self.disowned_jobs: set[UUID] = set()


def _make_ctx(
    buffers: dict[UUID, _ProgressBuffer],
) -> JobContext[BaseModel]:
    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_rt_test"})
    return make_progress_context(buffers, actor="rt_actor", worker_id=new_uuid(), settings=settings)


# ── Defect 1: progress(detail=...) bypasses the publish-time guard ──────


async def test_progress_data_with_surrogate_raises_at_publish() -> None:
    """GREEN pin of the sibling contract: ``data=`` carrying a lone
    surrogate is rejected by the publish-time serialization (the same
    dumps() call that enforces progress_data_max_bytes), so the poison
    never enters the coalesce buffer."""
    buf = _ProgressBuffer(job_id=new_job_id(), base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {buf.job_id: buf}
    ctx = _make_ctx(buffers)

    with pytest.raises(TypeError, match="surrogates not allowed"):
        await ctx.progress(data={"m": _SURROGATE})

    assert buf.pending_seq_delta == 0, "contract: a rejected publish leaves the buffer untouched"
    assert buf.dirty is False
    assert buf.pending_state == {}


async def test_progress_detail_with_surrogate_must_be_rejected_at_publish() -> None:
    """RED: ``detail=`` with the SAME unencodable string must be rejected
    at publish exactly like ``data=`` -- context.py serializes only
    ``data`` before buffering, so a surrogate detail silently enters
    pending_state and detonates later at the terminal write's jsonb_param
    (an unclassifiable TypeError, not a job failure)."""
    buf = _ProgressBuffer(job_id=new_job_id(), base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {buf.job_id: buf}
    ctx = _make_ctx(buffers)

    with pytest.raises(TypeError, match="surrogates not allowed"):
        await ctx.progress(detail=f"step {_SURROGATE} failed")

    assert buf.pending_state == {}, (
        f"contract: an unencodable detail must never reach the buffer; got {buf.pending_state!r}"
    )


async def test_surrogate_detail_must_not_break_the_terminal_write() -> None:
    """RED (end-to-end): an actor that publishes a surrogate detail and
    returns a storable result must still land 'succeeded' with the defect
    visible in the stored progress -- today the poisoned pending_state
    makes mark_succeeded's jsonb_param raise a bare TypeError that
    escapes the terminal-write classification (PostgresError/OSError/
    TimeoutError only), so the job is left running."""
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement - candidates come FROM the registry).
    backend.register_actor_config(actor="rt_detail_actor")
    args = make_enqueue_args(
        actor="rt_detail_actor", payload={"value": 1}, scheduled_at=backend._clock.now()
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    await backend.enqueue(args)
    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; the in-memory worker id owns the dispatch lease.
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    job: JobRow = dispatched[0]
    buffers: dict[UUID, _ProgressBuffer] = {}

    async def run_actor(_job: object, ctx: JobContext[BaseModel]) -> dict[str, object]:
        await ctx.progress(detail=f"processing {_SURROGATE}")
        return {"ok": True}

    outcome = await consume_one_job(
        backend,
        job,
        backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; matches the worker that holds the dispatch lease.
        run_actor=run_actor,
        actor_config=default_actor_config(),
        payload_type=PassthroughPayload,
        clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)),
        deps=_BufDeps(buffers),  # pyright: ignore[arg-type]  # Why: duck-typed WorkerDeps; the consumer reads only progress_buffers/pool/settings/redis/disowned_jobs off it.
    )
    assert outcome == "succeeded", (
        f"contract: a surrogate progress detail must not break the terminal write; outcome={outcome!r}"
    )
    stored = await backend.get(job.id)
    assert stored is not None
    assert stored.status == "succeeded", (
        f"contract: the job must land succeeded with the detail made visible, not strand; status={stored.status!r}"
    )


# ── Defect 2: unencodable actor result burns every retry attempt ────────


async def test_unencodable_result_is_non_retryable_like_result_too_large() -> None:
    """RED: an actor result containing a lone surrogate is a
    DETERMINISTIC serialization defect -- the actor already ran and a
    re-run returns the same unencodable value -- so it must fail
    non-retryably after exactly ONE execution (ResultTooLarge's exact
    contract, retry.py:351-357). Today the orjson TypeError classifies as
    a generic retryable failure and burns all three attempts."""
    runs: list[int] = []
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> dict[str, object]:  # Why: runner stub signature is fixed; both params are unused here.
        runs.append(1)
        return {"filename": _SURROGATE}

    backend.register_stub("rt_result_actor", stub)
    args = make_enqueue_args(
        actor="rt_result_actor",
        payload={"value": 1},
        max_attempts=3,
        scheduled_at=backend._clock.now(),  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; the established runner pattern.
    )
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert len(runs) == 1, (
        f"contract: a deterministic unencodable result is non-retryable -- one execution, like ResultTooLarge; actor ran {len(runs)} times"
    )
    assert stored.status == "failed", (
        f"contract: the job must be terminally failed, not stranded; status={stored.status!r}"
    )


# ── Defect 3's mirror half: surrogate exception MESSAGE ─────────────────


async def test_mirror_marks_failed_for_surrogate_exception_message() -> None:
    """GREEN pin (in-memory half): an actor raising an exception whose
    message carries a lone surrogate still lands 'failed' with the
    message preserved -- the mirror never UTF-8-encodes, so no strand.
    (The PG twin is RED: the same write raises asyncpg DataError, a
    PostgresError subclass the terminal-write classification misreads as
    transient infra -- see test_rt_payload_pg_jsonb.py.)"""
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> object:  # Why: runner stub signature is fixed; both params are unused here.
        raise ValueError(f"cannot open file {_SURROGATE}")

    backend.register_stub("rt_exc_actor", stub)
    args = make_enqueue_args(
        actor="rt_exc_actor",
        payload={"value": 1},
        max_attempts=1,
        scheduled_at=backend._clock.now(),
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; the established runner pattern.
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "failed", (
        f"contract: a surrogate-bearing exception message must still produce a terminal failure; status={stored.status!r}"
    )
    assert stored.error_class == "ValueError"
    assert stored.error_message is not None and _SURROGATE in stored.error_message, (
        "contract: the mirror preserves the unencodable message verbatim (the PG tier must sanitize instead of stranding)"
    )


async def test_failure_log_lines_survive_json_rendering() -> None:
    """The failure handler's log lines carry the exception's message and
    traceback, and a log chain may render through a strict UTF-8 encoder
    (taskq's own import-time chain renders every record through orjson).
    A lone surrogate in that text makes the render raise inside the
    handler -- before the terminal write -- and the job is left running.

    The durable row keeps the message verbatim (the pin above); the LOG
    channel's copy is the one that must be made renderable. Under the
    JSON chain the whole drain must complete and land the same row the
    tolerant chain lands.
    """
    # The import-time chain is installed only while structlog is
    # unconfigured; the suite's logging guard resets the configuration
    # around every test, so reinstall it here to run the drain under the
    # chain a plain ``import taskq`` produces.
    importlib.reload(taskq_obs_structlog)
    # The pin is only honest if the chain REALLY renders through the strict
    # encoder: an escaped message under a tolerant chain proves nothing.
    assert structlog.is_configured(), "the reload must leave structlog configured"
    chain_processors = structlog.get_config()["processors"]
    assert isinstance(chain_processors[-1], structlog.processors.JSONRenderer), (
        f"the reloaded chain must render through the strict JSON encoder; got {chain_processors[-1]!r}"
    )
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> object:  # Why: runner stub signature is fixed; both params are unused here.
        raise ValueError(f"cannot open file {_SURROGATE}")

    backend.register_stub("rt_exc_actor", stub, payload_type=PassthroughPayload)
    args = make_enqueue_args(
        actor="rt_exc_actor",
        payload={"value": 1},
        max_attempts=1,
        scheduled_at=backend._clock.now(),
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "failed", (
        f"contract: a JSON log chain must not strand the failure classification; status={stored.status!r}"
    )
    assert stored.error_class == "ValueError"
    assert stored.error_message is not None and _SURROGATE in stored.error_message, (
        "contract: the row keeps the message verbatim; only the log channel's copy is escaped"
    )


async def test_failure_error_class_survives_hostile_type_name() -> None:
    """The class name rides the durable row's ``error_class``, log lines and
    span attributes, and the actor's own exception class is attacker-adjacent
    code: a metaclass can raise on the ``__name__`` access itself or return
    a non-string, and either defect would kill the failure write and strand
    the job. (A surrogate in the name itself is impossible - CPython
    UTF-8-encodes type names at creation and at rename.) The guarded name
    is what the row stores per the mirror split: escaped on the log channel
    and PG-stored, while the message the row keeps stays verbatim."""
    importlib.reload(taskq_obs_structlog)
    assert structlog.is_configured()
    chain_processors = structlog.get_config()["processors"]
    assert isinstance(chain_processors[-1], structlog.processors.JSONRenderer), (
        f"the reloaded chain must render through the strict JSON encoder; got {chain_processors[-1]!r}"
    )
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    class _RaisingNameMeta(type):
        @property
        def __name__(cls) -> str:  # pyright: ignore[reportImplicitOverride, reportIncompatibleVariableOverride]  # Why: the metaclass property IS the hostile shape under test - a name access that raises; the override checks do not model metaclass name attributes.
            raise RuntimeError("the name access itself is the defect")

    raising_named = _RaisingNameMeta(
        "_Raising", (ValueError,), {}
    )  # Why: a local test class, not a constant.

    class _IntNameMeta(type):
        @property
        def __name__(cls) -> object:  # pyright: ignore[reportImplicitOverride, reportIncompatibleVariableOverride]  # Why: as above.
            return 7

    int_named = _IntNameMeta("_IntNamed", (ValueError,), {})  # Why: as above.

    def stub(
        payload: object, ctx: object
    ) -> object:  # Why: runner stub signature is fixed; both params are unused here.
        raise raising_named("the name access raises")

    backend.register_stub("rt_exc_actor", stub, payload_type=PassthroughPayload)
    args = make_enqueue_args(
        actor="rt_exc_actor",
        payload={"value": 1},
        max_attempts=1,
        scheduled_at=backend._clock.now(),
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "failed", (
        f"contract: a raising type-name access must not strand the classification; status={stored.status!r}"
    )
    assert stored.error_class == "<exception type name failed>", (
        f"contract: a raising name access degrades to the marker; got {stored.error_class!r}"
    )

    def int_stub(
        payload: object, ctx: object
    ) -> object:  # Why: runner stub signature is fixed; both params are unused here.
        raise int_named("the name is not a string")

    backend.register_stub("rt_exc_actor", int_stub, payload_type=PassthroughPayload)
    args = make_enqueue_args(
        actor="rt_exc_actor",
        payload={"value": 1},
        max_attempts=1,
        scheduled_at=backend._clock.now(),
    )  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "failed", (
        f"contract: a non-string type name must not strand the classification; status={stored.status!r}"
    )
    assert stored.error_class == "7", (
        f"contract: a non-string name is coerced; got {stored.error_class!r}"
    )


async def test_terminal_write_failure_log_survives_json_rendering() -> None:
    """A terminal write that fails with an infra exception leaves the row
    ``running`` (lock-lease expiry reclaims it) and logs the failure — and
    that log runs INSIDE the terminal-write failure handler, so a surrogate
    riding the infra exception's text must not raise through the handler
    and strand the drain itself."""
    importlib.reload(taskq_obs_structlog)
    assert structlog.is_configured()
    chain_processors = structlog.get_config()["processors"]
    assert isinstance(chain_processors[-1], structlog.processors.JSONRenderer), (
        f"the reloaded chain must render through the strict JSON encoder; got {chain_processors[-1]!r}"
    )
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> object:  # Why: runner stub signature is fixed; both params are unused here.
        return {"ok": True}

    backend.register_stub("rt_exc_actor", stub, payload_type=PassthroughPayload)
    args = make_enqueue_args(
        actor="rt_exc_actor",
        payload={"value": 1},
        max_attempts=1,
        scheduled_at=backend._clock.now(),
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    row = await backend.enqueue(args)

    async def failing_mark_succeeded(*a: object, **k: object) -> JobRow:
        raise asyncpg.PostgresError(f"simulated write failure {_SURROGATE}")

    backend.mark_succeeded = failing_mark_succeeded  # type: ignore[method-assign]  # Why: the pin drives the terminal-write failure shape the in-memory backend cannot reach through its own storage.
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "running", (
        f"contract: a failed terminal write leaves the row for the reclaim sweep; status={stored.status!r}"
    )


async def test_span_survives_surrogate_exception() -> None:
    """The attempt span records the exception event and its ERROR status
    from the same rendered text the log channels get. The OTLP exporter
    protobuf-encodes span attribute strings, and a lone surrogate would
    drop the span at export - the span must carry the ESCAPED text, the
    span itself never dropped."""
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    span = tracer.start_span("attempt.1")
    record_exception_text(span, render_exception(ValueError(f"cannot open file {_SURROGATE}")))
    span.end()
    provider.force_flush()

    exported = exporter.get_finished_spans()
    assert len(exported) == 1, "contract: the span is exported, not dropped"
    events = exported[0].events
    assert any(e.name == "exception" for e in events), (
        "contract: the exception event is on the span"
    )
    event = next(e for e in events if e.name == "exception")
    attributes = event.attributes or {}
    message = attributes["exception.message"]
    assert isinstance(message, str) and _SURROGATE not in message
    assert "\\udcff" in message, f"contract: the span carries the escaped form; got {message!r}"
    message.encode("utf-8")  # the protobuf face: the attribute must be UTF-8 encodable


async def test_span_survives_surrogate_type_name() -> None:
    """The exception's class QUALNAME, unlike its name, can carry a lone
    surrogate (CPython locks type names to UTF-8 but accepts any string in
    ``__qualname__``) - and the qualname rides ``exception.type`` on the
    span event. The exported attribute must be the escaped form."""
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")

    surrogate_qualified = type("_SurrogateQualified", (ValueError,), {})
    surrogate_qualified.__qualname__ = f"bad{_SURROGATE}qual"

    span = tracer.start_span("attempt.1")
    record_exception_text(span, render_exception(surrogate_qualified("the message")))
    span.end()
    provider.force_flush()

    exported = exporter.get_finished_spans()
    assert len(exported) == 1, "contract: the span is exported, not dropped"
    event = next(e for e in exported[0].events if e.name == "exception")
    attributes = event.attributes or {}
    type_attr = attributes["exception.type"]
    assert isinstance(type_attr, str) and _SURROGATE not in type_attr
    assert "\\udcff" in type_attr, f"contract: exception.type is stored escaped; got {type_attr!r}"


async def test_generic_error_status_description_is_utf8_valid() -> None:
    """The generic error-status path (a span whose status the call site did
    not set) writes the scrubbed message as the ERROR description - and the
    description is exported through the same protobuf face. A surrogate in
    the actor exception's message must not survive into it."""
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")

    from taskq.obs._otel import _record_scrubbed_error

    span = tracer.start_span("attempt.1")
    _record_scrubbed_error(span, ValueError(f"cannot open file {_SURROGATE}"))
    span.end()
    provider.force_flush()

    exported = exporter.get_finished_spans()
    assert len(exported) == 1, "contract: the span is exported, not dropped"
    status = exported[0].status
    assert status.status_code.name == "ERROR"
    description = status.description
    assert isinstance(description, str) and _SURROGATE not in description
    assert "\\udcff" in description, (
        f"contract: the status description carries the escaped form; got {description!r}"
    )
    description.encode("utf-8")


async def test_render_survives_hostile_identity_access() -> None:
    """A metaclass can raise on the exception's identity access itself - the
    traceback formatter reads the class qualname for the header line, so an
    unguarded render dies inside the failure handler that was classifying
    the exception and strands the row it was reporting. The render degrades:
    the marker for every field the hostile access touches, the message (a
    guarded str) still rendered."""
    importlib.reload(taskq_obs_structlog)
    assert structlog.is_configured()
    chain_processors = structlog.get_config()["processors"]
    assert isinstance(chain_processors[-1], structlog.processors.JSONRenderer), (
        f"the reloaded chain must render through the strict JSON encoder; got {chain_processors[-1]!r}"
    )

    class _Meta(type):
        def __getattribute__(cls, item: str) -> object:
            if item == "__qualname__":
                raise RuntimeError("the qualname access itself is the defect")
            return super().__getattribute__(item)

    hostile = _Meta("_HostileIdentity", (ValueError,), {})
    text = render_exception(hostile("the message"))
    assert text.type_name == "<exception type name failed>"
    assert text.raw_stacktrace == "<exception traceback rendering failed>"

    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> object:  # Why: runner stub signature is fixed; both params are unused here.
        raise hostile("the message")

    backend.register_stub("rt_exc_actor", stub, payload_type=PassthroughPayload)
    args = make_enqueue_args(
        actor="rt_exc_actor",
        payload={"value": 1},
        max_attempts=1,
        scheduled_at=backend._clock.now(),
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "failed", (
        f"contract: a hostile identity access must not strand the classification; status={stored.status!r}"
    )
    # The class NAME access is untouched by this metaclass (it only breaks
    # the qualname), so the row's error_class reads the real name; the
    # RENDER's degradation is what kept the drain alive.
    assert stored.error_class == "_HostileIdentity"
    assert "the message" in (stored.error_message or "")
