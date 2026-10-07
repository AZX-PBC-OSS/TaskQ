"""Pins for strict queue-name validation (``TASKQ_QUEUES_STRICT``).

Red (proven live at the audit base b0504fb2): with strict mode ON, a
submit to a typo'd queue name stored the row silently -- ``pending``
forever, no worker ever claims it, NOTHING signaled at submit time.
The typo is one character; the failure is silent data non-delivery.
The chosen fix: the configured queue set (``TASKQ_QUEUES``, now a
base-settings field so the CLIENT process knows it too -- the
split-deployment trap: actors register ONLY in the worker, so
client-side validation may consult configuration, never actor
registration) becomes a hard opt-in gate at the submit chokepoint.
An unknown name raises :class:`~taskq.exceptions.UnknownQueueError`
naming the offending queue AND the configured set, and nothing is
stored -- the orphaned message is refused at the door instead of
parked invisibly.

Deliberate postures pinned here:

* **Default never mutates explicit intent.** Strict is OFF by default;
  every non-strict behavior (including the enqueue-time unserved-queue
  NOTE from test_enqueue_unserved_queue_note.py, which strict sits
  ABOVE as the hard opt-in) is pinned unchanged.
* **The escape hatch is the wildcard entry** (``*`` in the configured
  set), not a per-submit parameter: internal submit paths (cron fires,
  fan-out children, retries) have no caller to pass an override, and a
  config-level escape covers every arm through the same seam with zero
  API growth. The wildcard is a VALIDATION-SET entry only -- it never
  names a physical queue (the worker's consumed list strips it).
* **Client-side checks never consult actor registration** (the web
  process has none); the actor-registration-shaped check (a configured
  queue nothing routes to) is the WORKER-side startup gate, fail-fast
  there and only there.
"""

from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.client import JobsClient, SubJobEnqueuer
from taskq.exceptions import TaskQError, UnknownQueueError
from taskq.retry import RetryPolicy
from taskq.settings import TaskQSettings, WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


class _Result(BaseModel):
    ok: bool = True


def _make_ref(name: str = "work", queue: str = "email") -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()  # pragma: no cover - never dispatched in this test

    _handler.__qualname__ = f"_handler_{name}"  # type: ignore[misc]
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


def _strict_settings(
    queues: str = "email,weather",
    *,
    strict: bool = True,
) -> TaskQSettings:
    flags = "true" if strict else "false"
    return TaskQSettings.load_from_dict(
        {
            "TASKQ_QUEUES": queues,
            "TASKQ_QUEUES_STRICT": flags,
        }
    )


# ── The orphaned-message repro ────────────────────────────────────────


async def test_strict_submit_to_unknown_queue_raises_and_stores_nothing() -> None:
    """THE RED: strict on, a one-character typo'd queue name raised
    NOTHING and stored the row anyway -- pending forever, no worker
    claims it, silent data non-delivery. The green: the refusal fires
    at the submit site and the row is NOT stored."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref()
    with pytest.raises(UnknownQueueError):
        await client.enqueue(ref, _Payload(), queue="emial")
    # The orphan is never written: nothing sits pending on a queue no
    # worker claims.
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row; the store is the backend's private dict.


async def test_strict_error_names_the_queue_and_the_configured_set() -> None:
    """The error's quality bar (the unserved-queue note's): the fix must
    be obvious from the message alone -- it names the offending queue,
    the configured set it is judged against, and the strict knob."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref()
    with pytest.raises(UnknownQueueError) as excinfo:
        await client.enqueue(ref, _Payload(), queue="emial")
    message = str(excinfo.value)
    assert "emial" in message, message
    assert "email" in message, "the configured set is named"
    assert "weather" in message, "the configured set is named"
    assert "TASKQ_QUEUES" in message, message
    assert isinstance(excinfo.value, TaskQError)


async def test_strict_valid_queue_still_enqueues() -> None:
    """A queue in the configured set enqueues normally under strict."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="weather")
    assert handle.row.status == "pending"


async def test_default_off_typo_stores_silently_with_the_note() -> None:
    """Default never mutates explicit intent: strict OFF (the default),
    the same typo'd submit still stores the row and the unserved-queue
    NOTE (the diagnostic strict sits above) still fires. No exception."""
    import structlog

    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings(strict=False))
    ref = _make_ref()
    with structlog.testing.capture_logs() as logs:
        handle = await client.enqueue(ref, _Payload(), queue="emial")
    assert handle.row.status == "pending"
    assert any(e.get("event") == "enqueue-unserved-queue" for e in logs)


# ── The escape hatch: the wildcard entry ──────────────────────────────


async def test_wildcard_entry_allows_dynamic_queues() -> None:
    """The escape hatch: ``*`` in the configured set opens strict mode
    to genuinely dynamic queue names, process-wide, one env line."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings(queues="email,*"))
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="tenant_42_dyn")
    assert handle.row.status == "pending"


async def test_wildcard_is_a_validation_entry_never_a_queue_name() -> None:
    """The wildcard never names a physical queue: the worker's consumed
    list strips it, so the dispatch claim can never match a job whose
    queue is literally ``*``."""
    from taskq._queue_policy import QueuePolicy

    assert QueuePolicy.consumed_queues(["email", "*"]) == ["email"]
    assert QueuePolicy.consumed_queues(["*"]) == []
    assert QueuePolicy.consumed_queues(["email"]) == ["email"]


# ── The chokepoint covers every submit arm ────────────────────────────


def _item(ref: ActorRef[_Payload, _Result]) -> Any:
    from taskq.batch import EnqueueItem

    return EnqueueItem(actor_ref=ref, payload=_Payload())


async def test_batch_arm_refuses_unknown_queue() -> None:
    """The batch arm speaks the same contract: an item whose ref routes
    to an unknown queue raises before any row is written."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch([_item(ref)])
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_streaming_arm_refuses_unknown_queue() -> None:
    """The streaming arm (sync generator, mid-transaction) refuses the
    same way: the verdict is a pure snapshot lookup, no await needed."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref(queue="emial")

    def _stream() -> Any:
        yield _item(ref)

    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch_streaming(_stream())
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_fast_arm_refuses_unknown_queue() -> None:
    """The COPY FROM arm refuses too: bulk throughput is not a
    validation bypass."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch_fast([_item(ref)])
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_sub_enqueuer_fanout_refuses_unknown_queue() -> None:
    """Fan-out children (``ctx.jobs.enqueue`` inside an actor body) go
    through the same seam: a child routed to an unknown queue raises in
    the actor body instead of storing an orphan."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # type: ignore[arg-type]  # Why: SubJobEnqueuer only None-checks the pool (the autonomous-fallback gate); the in-memory backend has no asyncpg pool.
        backend=backend,
        queue_policy=_strict_settings_policy(),
    )
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await enqueuer.enqueue(ref, _Payload())


async def test_sub_enqueuer_batch_fanout_refuses_unknown_queue() -> None:
    """The enqueuer's batch arm (fan-out children in one call) refuses
    the same way."""
    from taskq.batch import EnqueueItem

    backend = InMemoryBackend(clock=FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # type: ignore[arg-type]  # Why: SubJobEnqueuer only None-checks the pool (the autonomous-fallback gate); the in-memory backend has no asyncpg pool.
        backend=backend,
        queue_policy=_strict_settings_policy(),
    )
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await enqueuer.enqueue_batch([EnqueueItem(actor_ref=ref, payload=_Payload())])


def _strict_settings_policy() -> Any:
    from taskq._queue_policy import QueuePolicy

    return QueuePolicy.from_settings(_strict_settings())


async def test_sub_enqueuer_without_policy_stays_silent() -> None:
    """Fail-open posture pinned: an enqueuer built without a policy
    (no settings reached it) enqueues without enforcement -- strict
    requires a configured set, and a bare construction never invents
    one. The unserved-queue NOTE remains the signal there."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # type: ignore[arg-type]  # Why: SubJobEnqueuer only None-checks the pool (the autonomous-fallback gate); the in-memory backend has no asyncpg pool.
        backend=backend,
    )
    ref = _make_ref(queue="emial")
    handle = await enqueuer.enqueue(ref, _Payload())
    assert handle.row.status == "pending"


# ── The worker-side startup gate (split-deployment safe) ──────────────


def _worker_settings(queues: str = "email,ghost", *, strict: bool = True) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_QUEUES": queues,
            "TASKQ_QUEUES_STRICT": "true" if strict else "false",
        }
    )


def _registry(*queues: str) -> dict[str, ActorRef[_Payload, _Result]]:
    return {f"actor_{q}": _make_ref(name=f"actor_{q}", queue=q) for q in queues}


def test_worker_startup_gate_fails_fast_on_unrouted_queue() -> None:
    """Strict's worker-side half: a configured queue nothing routes to
    (no stored assignment, no registered literal) fails the boot, the
    config-drift catch the issue asks for. WORKER-side only: the client
    process has no actor registry to consult."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = _worker_settings(queues="email,ghost")
    registry = _registry("email")  # nothing routes to ghost
    with pytest.raises(UnknownQueueError) as excinfo:
        _fail_fast_on_unrouted_configured_queues(
            settings, registry, structlog.get_logger("test"), stored_queues={}
        )
    message = str(excinfo.value)
    assert "ghost" in message, message
    assert "email" in message, "the configured set is named"
    assert "TASKQ_QUEUES" in message, message


def test_worker_startup_gate_passes_when_every_configured_queue_is_routed() -> None:
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = _worker_settings(queues="email,weather")
    registry = _registry("email", "weather")
    _fail_fast_on_unrouted_configured_queues(
        settings, registry, structlog.get_logger("test"), stored_queues={}
    )


def test_worker_startup_gate_off_by_default() -> None:
    """Default never mutates explicit intent: without strict, the same
    unrouted configuration boots (the existing aggregated WARNING keeps
    owning that signal)."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = _worker_settings(queues="email,ghost", strict=False)
    registry = _registry("email")
    _fail_fast_on_unrouted_configured_queues(
        settings, registry, structlog.get_logger("test"), stored_queues={}
    )


def test_worker_startup_gate_wildcard_disables_it() -> None:
    """The escape hatch reaches the worker gate: a wildcard in the
    configured set disables the fail-fast (dynamic fleets judge nothing
    at boot)."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = _worker_settings(queues="*,ghost")
    registry = _registry("email")
    _fail_fast_on_unrouted_configured_queues(
        settings, registry, structlog.get_logger("test"), stored_queues={}
    )


def test_worker_startup_gate_stored_assignment_provides_coverage() -> None:
    """The stored (operator-owned) assignment is what routes cron fires
    and re-pended rows: an actor whose stored row routes a configured
    queue covers it even when this process's literal disagrees (the
    same precedence the unconsumed-queue warning uses)."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = _worker_settings(queues="email")
    registry = _registry()  # empty registry: only the stored row routes
    _fail_fast_on_unrouted_configured_queues(
        settings, registry, structlog.get_logger("test"), stored_queues={"actor_email": "email"}
    )


# ── The settings knob ─────────────────────────────────────────────────


def test_knob_defaults_off_and_lives_on_the_base_settings() -> None:
    """The knob follows the settings pattern: a dotenvmodel field on the
    BASE class (the client process must know the configured set too --
    the split-deployment constraint), off by default, and TASKQ_QUEUES
    stays loadable from the worker class."""
    defaults = TaskQSettings.load_from_dict({})
    assert defaults.queues == ["default"]
    assert defaults.queues_strict is False
    on = _strict_settings()
    assert on.queues == ["email", "weather"]
    assert on.queues_strict is True
    worker = _worker_settings()
    assert worker.queues == ["email", "ghost"]
    assert worker.queues_strict is True


def test_unknown_queue_error_is_exported_from_taskq() -> None:
    """The error type is the export surface, nothing else gratuitous."""
    import taskq

    assert taskq.UnknownQueueError is UnknownQueueError
    assert "UnknownQueueError" in taskq.__all__


# ── Integration (real backend) ────────────────────────────────────────


@pytest.mark.integration
async def test_strict_submit_refuses_against_pg(jobs_app: JobsApp) -> None:
    """The red, proven against the real backend: one strict submit to a
    typo'd queue raises at the door and stores nothing."""
    client = JobsClient(
        jobs_app.backend,
        settings=_strict_settings(),
    )
    ref = _make_ref()
    with pytest.raises(UnknownQueueError):
        await client.enqueue(ref, _Payload(), queue="emial")
