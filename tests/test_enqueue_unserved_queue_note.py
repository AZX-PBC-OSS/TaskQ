"""Pins for the enqueue-time unserved-queue note.

Red (proven live at the audit base): an enqueue onto a queue no worker
consumes succeeded silently -- the row sat ``pending`` forever and
NOTHING signaled at enqueue time. The chosen fix (the perf-law choice:
a naive per-enqueue ``queues`` probe measured +10.1% per enqueue --
one extra round trip on the hot path -- so the signal rides the
client-side ``ActorCapacityCache`` snapshot instead): the cache now
also snapshots ``actor_config``'s queue assignments on the SAME
refresh cadence (one small whole-table read per TTL beside the
max_pending read, never per enqueue), and every enqueue arm consults
the snapshot with zero I/O, warning once per queue per TTL when no
registered actor routes to the queue.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
import structlog
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.client import JobsClient
from taskq.retry import RetryPolicy
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


def _configure(backend: InMemoryBackend, ref: ActorRef[Any, Any]) -> None:
    """Register the ref's actor_config meta: the role of a stored row."""
    from taskq.actor_config import ActorConfig

    backend.register_actor_configs(
        [
            ActorConfig(
                actor=ref.name,
                max_concurrent=None,
                queue=ref.queue,
            )
        ]
    )


@pytest.fixture
def log_events() -> Iterator[list[structlog.types.EventDict]]:
    with structlog.testing.capture_logs() as logs:
        yield logs


def _warnings(
    logs: list[structlog.types.EventDict], name: str = "enqueue-unserved-queue"
) -> list[structlog.types.EventDict]:
    return [e for e in logs if e.get("event") == name]


async def test_ghost_queue_enqueue_warns(log_events: list[structlog.types.EventDict]) -> None:
    """The red's flip: enqueue to a queue nothing registers still
    succeeds, but the note now fires, naming the actor and the queue."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"
    warnings = _warnings(log_events)
    assert len(warnings) == 1
    assert warnings[0]["queue"] == "ghost_queue"
    assert warnings[0]["actor"] == ref.name


async def test_served_queue_enqueue_stays_silent(
    log_events: list[structlog.types.EventDict],
) -> None:
    """A queue a registered actor routes to never warns."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    ref = _make_ref(queue="email")
    _configure(backend, ref)
    client = JobsClient(backend)
    await client.enqueue(ref, _Payload())
    assert _warnings(log_events) == []


async def test_note_reason_names_the_queues_consumer_escape(
    log_events: list[structlog.types.EventDict],
) -> None:
    """Red: the predicate is the stored-assignment set, so a worker
    consuming the queue via ``--queues``/``TASKQ_QUEUES`` (no stored
    assignment routes there) makes the note fire on a job that WILL be
    dispatched. The note may not fire at all in that corner (the
    docstring owns that trade), but its reason must not assert certain
    stranding: it names the consumer escape so the false positive is
    self-explaining instead of sending an operator hunting for a
    stranded row that does not exist."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)
    ref = _make_ref()
    await client.enqueue(ref, _Payload(), queue="ghost_queue")
    (warning,) = _warnings(log_events)
    reason = str(warning["reason"])
    assert "--queues" in reason
    assert "TASKQ_QUEUES" in reason
    # The stranded claim is conditional, never absolute.
    assert "stays pending until" not in reason


async def test_warn_once_per_queue_per_ttl(
    log_events: list[structlog.types.EventDict],
) -> None:
    """A hot producer dumping thousands of rows onto one typo'd queue
    must not flood the log: one warning per queue per TTL window."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)
    ref = _make_ref()
    for _ in range(5):
        await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert len(_warnings(log_events)) == 1


async def test_refresh_failure_fails_open(
    log_events: list[structlog.types.EventDict],
) -> None:
    """A sick backend disables the note (fail-open, the capacity cache's
    documented posture): the enqueue succeeds, no unserved-queue warning,
    the refresh failure carries the diagnosis instead."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    class _Sick(InMemoryBackend):
        async def get_actor_max_pending(self) -> dict[str, int | None]:
            raise RuntimeError("backend down")

    sick = _Sick.__new__(_Sick)
    sick.__dict__.update(backend.__dict__)
    client = JobsClient(sick)
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"
    assert _warnings(log_events) == []
    assert any(e.get("event") == "actor-capacity-cache-refresh-failed" for e in log_events)


async def test_non_dict_capability_payload_fails_open(
    log_events: list[structlog.types.EventDict],
) -> None:
    """A backend whose get_actor_queues returns a non-dict (a mock's
    auto-vivified child, a contract bug) must not turn the note's lookup
    into a hot-path TypeError: the snapshot fails open and the refresh
    failure carries the diagnosis."""
    client = JobsClient(_drifted(InMemoryBackend(clock=FakeClock(_NOW))))
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"
    assert _warnings(log_events) == []
    assert any(e.get("event") == "actor-queue-snapshot-refresh-failed" for e in log_events)


def _drifted(inner: InMemoryBackend) -> InMemoryBackend:
    """An InMemoryBackend whose get_actor_queues returns a non-dict."""
    drifted = InMemoryBackend(clock=FakeClock(_NOW))
    drifted.__dict__.update(inner.__dict__)

    async def bad_queues() -> dict[str, str]:
        return "not a dict"  # type: ignore[return-value]  # Why: the contract drift IS the case under test.

    drifted.get_actor_queues = bad_queues  # type: ignore[method-assign]
    return drifted


async def test_backend_without_the_capability_never_crashes(
    log_events: list[structlog.types.EventDict],
) -> None:
    """``get_actor_queues`` is an OPTIONAL backend capability: a protocol
    v3 backend without it enqueues normally, the note simply stays off."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    class _V3Only:
        def __init__(self, inner: InMemoryBackend) -> None:
            self._inner = inner

        def __getattr__(self, name: str) -> Any:
            if name == "get_actor_queues":
                # Structural absence: the capability this double is built
                # to omit must not fall through to the inner backend.
                raise AttributeError(name)
            return getattr(self._inner, name)

        # The one capability protocol v3 pins; deliberately no
        # get_actor_queues.
        async def get_actor_max_pending(self) -> dict[str, int | None]:
            return await self._inner.get_actor_max_pending()

    client = JobsClient(_V3Only(backend))
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"
    assert _warnings(log_events) == []


async def test_no_read_amplification_on_the_hot_path(
    log_events: list[structlog.types.EventDict],
) -> None:
    """The perf-law pin: the note costs ZERO backend round trips at
    enqueue time. The second enqueue inside the TTL window performs NO
    backend read at all (the snapshot from the first enqueue serves
    both), and the note itself is a pure snapshot lookup."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)
    ref = _make_ref()

    reads = {"max_pending": 0, "queues": 0}
    inner_get_mp = backend.get_actor_max_pending
    inner_get_q = backend.get_actor_queues

    async def counting_mp() -> dict[str, int | None]:
        reads["max_pending"] += 1
        return await inner_get_mp()

    async def counting_q() -> dict[str, str]:
        reads["queues"] += 1
        return await inner_get_q()

    backend.get_actor_max_pending = counting_mp  # type: ignore[method-assign]
    backend.get_actor_queues = counting_q  # type: ignore[method-assign]

    await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert reads == {"max_pending": 1, "queues": 1}

    with structlog.testing.capture_logs() as second_round_logs:
        await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert reads == {"max_pending": 1, "queues": 1}, "a warm-cache enqueue must not re-read"
    assert _warnings(second_round_logs) == [], "warn-once per TTL holds on the warm path"


@pytest.mark.integration
async def test_ghost_queue_warns_against_pg(
    jobs_app: JobsApp, log_events: list[structlog.types.EventDict]
) -> None:
    """The red, proven against the real backend: a fresh schema (no
    stored rows, no workers), one enqueue to an unregistered queue, the
    note fires and the row sits pending."""
    client = JobsClient(jobs_app.backend)
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"
    warnings = _warnings(log_events)
    assert len(warnings) == 1
    assert warnings[0]["queue"] == "ghost_queue"
