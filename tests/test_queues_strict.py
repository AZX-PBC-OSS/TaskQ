"""Pins for strict queue-name validation (``TASKQ_QUEUES_STRICT``).

Red (proven live at the audit base b0504fb2): with the strict knob set,
a submit to a typo'd queue name stored the row silently -- ``pending``
forever, no worker ever claims it, NOTHING raised at submit time. The
typo is one character; the failure is silent data non-delivery.

The design (the review-corrected one): strict is the HARD BRANCH on
``ActorCapacityCache.maybe_warn_unserved_queue`` -- the zero-I/O
TTL-snapshot predicate every enqueue arm already consults (direct, the
three batch arms, the sub-job fan-out enqueuer). No parallel validation
path exists. The judged set is the REGISTERED queue assignments (the
fleet-wide ``actor_config`` DB truth the client reads without actor
registration -- the split-deployment constraint: the web process
submits, the worker consumes, actors register ONLY in the worker, so
the client-side gate consults configuration/stored truth, never a
registry). The worker-side boot gate (a configured queue nothing routes
to) runs in the worker bootstrap ONLY, after ``sync_actor_config``'s
fleet-wide read-back.

Postures pinned here:

* **Fail-open on an UNAVAILABLE snapshot** (the note's own documented
  posture, inherited): a refresh blip or a capability-less backend is
  not evidence of stranding -- strict must not turn a database blip
  into a submit outage. A POSITIVE snapshot verdict (queue present,
  not routed) is what refuses.
* **Default never mutates explicit intent**: strict OFF by default;
  the non-strict behaviors (including the unserved-queue NOTE, the
  diagnostic strict sits above) are pinned unchanged.
* **The escape hatch is the per-submit override**
  (``allow_unregistered=True``), chosen over a config wildcard: the
  gate's set is the registered-assignment snapshot, which internal
  paths populate BY CONSTRUCTION (cron fires route via the stored
  ``actor_config`` assignment -- the snapshot's own source; row flips
  and snooze-drain re-enqueues reuse already-checked args), so the only
  callers who can ever hit the refusal are user-facing submit sites --
  exactly where a per-call flag is available and precise. The
  process-wide escape is the knob itself.
* **Row-creating bypass paths are exempt by construction**, each with
  its pin: cron-fired instances enqueue the stored assignment's queue
  (the cron tick writes through the backend, never the client arms,
  and its queue IS the snapshot's source); the consumer's
  Snooze/RetryAfter drain re-enqueues the drained args, which rode the
  check at buffer time -- a refused child never even enters the buffer.
"""

from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest
import structlog
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.batch import EnqueueItem
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


def _strict_settings(**overrides: str) -> TaskQSettings:
    return TaskQSettings.load_from_dict({"TASKQ_QUEUES_STRICT": "true", **overrides})


def _configure(backend: InMemoryBackend, ref: ActorRef[Any, Any]) -> None:
    """Register the ref's actor_config meta: the registration a worker's
    sync writes, the stored assignment the snapshot reads."""
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


# ── The orphaned-message repro ────────────────────────────────────────


async def test_strict_submit_to_unknown_queue_raises_and_stores_nothing() -> None:
    """THE RED: strict on, a one-character typo'd queue name raised
    NOTHING and stored the row anyway -- pending forever, no worker
    claims it, silent data non-delivery. The green: the refusal fires
    at the submit site and the row is NOT stored.

    Steady-state fleet (a served route registered, the snapshot
    non-empty): the asymmetry doctrine's empty-snapshot rule disables
    the refusal when NOTHING is registered (the first deploy — its own
    pin); positive two-source evidence is what refuses here."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    served = _make_ref(queue="email")
    _configure(backend, served)
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref()
    with pytest.raises(UnknownQueueError):
        await client.enqueue(ref, _Payload(), queue="emial")
    # The orphan is never written: nothing sits pending on a queue no
    # worker claims.
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row; the store is the backend's private dict.


async def test_strict_error_names_the_queue_and_the_judged_set() -> None:
    """The error's quality bar (the unserved-queue note's): the fix must
    be obvious from the message alone -- it names the offending queue,
    the registered routes it was judged against, the TTL bound, and the
    strict knob."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    # A registered route exists (the judged set is non-empty) so the
    # message can be pinned to NAME it.
    served = _make_ref(name="served", queue="email")
    _configure(backend, served)
    ref = _make_ref()
    with pytest.raises(UnknownQueueError) as excinfo:
        await client.enqueue(ref, _Payload(), queue="emial")
    message = str(excinfo.value)
    assert "emial" in message, message
    assert "email" in message, "the judged set is named"
    assert "TASKQ_QUEUES_STRICT" in message, message
    assert "allow_unregistered" in message, "the escape is named"
    assert "TTL" in message, "the staleness bound is disclosed"
    assert isinstance(excinfo.value, TaskQError)


async def test_strict_registered_queue_still_enqueues() -> None:
    """A queue a registered actor routes to enqueues normally under
    strict: the gate judges the stored assignment, and a served queue
    passes."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    ref = _make_ref(queue="weather")
    _configure(backend, ref)
    client = JobsClient(backend, settings=_strict_settings())
    handle = await client.enqueue(ref, _Payload(), queue="weather")
    assert handle.row.status == "pending"


async def test_default_off_typo_stores_silently_with_the_note() -> None:
    """Default never mutates explicit intent: strict OFF (the default),
    the same typo'd submit still stores the row and the unserved-queue
    NOTE (the diagnostic strict sits above) still fires. No exception."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    # The knob's DEFAULT is off; loading with strict=true above and
    # false here would be two loads -- pin the default literally.
    default = TaskQSettings.load_from_dict({})
    assert default.queues_strict is False
    client = JobsClient(backend, settings=default)
    ref = _make_ref()
    with structlog.testing.capture_logs() as logs:
        handle = await client.enqueue(ref, _Payload(), queue="emial")
    assert handle.row.status == "pending"
    assert any(e.get("event") == "enqueue-unserved-queue" for e in logs)


# ── Fail-open on an unavailable snapshot (the critical posture) ───────


async def test_strict_fails_open_on_snapshot_read_failure() -> None:
    """A snapshot-read blip is not evidence of stranding: strict on, the
    refresh fails, the enqueue SUCCEEDS -- no UnknownQueueError, the
    refresh-failure event carries the diagnosis instead. Strict refuses
    on a POSITIVE verdict only."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    class _Sick(InMemoryBackend):
        async def get_actor_max_pending(self) -> dict[str, int | None]:
            raise RuntimeError("backend down")

    sick = _Sick.__new__(_Sick)
    sick.__dict__.update(backend.__dict__)
    client = JobsClient(sick, settings=_strict_settings())
    ref = _make_ref()
    with structlog.testing.capture_logs() as logs:
        handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"
    assert not any(e.get("event") == "enqueue-unserved-queue" for e in logs)
    assert any(e.get("event") == "actor-capacity-cache-refresh-failed" for e in logs)


async def test_strict_fails_open_on_capability_less_backend() -> None:
    """A backend without the optional ``get_actor_queues`` capability
    carries no snapshot: strict stays off for it (the note's own staged
    posture), the enqueue proceeds."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    class _V3Only:
        def __init__(self, inner: InMemoryBackend) -> None:
            self._inner = inner

        def __getattr__(self, name: str) -> Any:
            if name == "get_actor_queues":
                raise AttributeError(name)
            return getattr(self._inner, name)

        async def get_actor_max_pending(self) -> dict[str, int | None]:
            return await self._inner.get_actor_max_pending()

    client = JobsClient(_V3Only(backend), settings=_strict_settings())
    ref = _make_ref()
    handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"


async def test_strict_escape_still_fires_the_note() -> None:
    """The escape is not a silencer: allow_unregistered=True skips the
    refusal but the unserved-queue NOTE still fires, so a typo'd name
    stays visible even on an escaped call."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref()
    with structlog.testing.capture_logs() as logs:
        handle = await client.enqueue(
            ref, _Payload(), queue="tenant_42_dyn", allow_unregistered=True
        )
    assert handle.row.status == "pending"
    assert any(e.get("event") == "enqueue-unserved-queue" for e in logs)


async def test_escape_flag_changes_nothing_when_strict_is_off() -> None:
    """With strict off (the default) the flag is inert: same behavior,
    same note, either way."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=TaskQSettings.load_from_dict({}))
    ref = _make_ref()
    with structlog.testing.capture_logs() as logs:
        handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
    assert handle.row.status == "pending"
    assert any(e.get("event") == "enqueue-unserved-queue" for e in logs)


# ── The chokepoint covers every submit arm ────────────────────────────


def _item(ref: ActorRef[_Payload, _Result]) -> Any:
    return EnqueueItem(actor_ref=ref, payload=_Payload())


async def test_batch_arm_refuses_unknown_queue() -> None:
    """The batch arm speaks the same contract: an item whose ref routes
    to an unregistered queue raises before any row is written."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    served = _make_ref(queue="email")
    _configure(backend, served)  # non-empty snapshot: positive evidence available
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch([_item(ref)])
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_batch_arm_registered_queue_enqueues() -> None:
    """The batch arm's green twin: registered routes flow."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    ref = _make_ref(queue="email")
    _configure(backend, ref)
    client = JobsClient(backend, settings=_strict_settings())
    handle_batch = await client.enqueue_batch([_item(ref)])
    assert handle_batch.job_handles[0].row.status == "pending"


async def test_streaming_arm_refuses_unknown_queue() -> None:
    """The streaming arm (sync generator, mid-transaction) refuses the
    same way: the verdict is a pure snapshot lookup, no await needed."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    served = _make_ref(queue="email")
    _configure(backend, served)  # non-empty snapshot: positive evidence available
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
    served = _make_ref(queue="email")
    _configure(backend, served)  # non-empty snapshot: positive evidence available
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch_fast([_item(ref)])
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_sub_enqueuer_fanout_refuses_unknown_queue() -> None:
    """Fan-out children (``ctx.jobs.enqueue`` inside an actor body) ride
    the same seam: a child routed to an unregistered queue raises in the
    actor body instead of storing an orphan."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    served = _make_ref(queue="email")
    _configure(backend, served)  # non-empty snapshot: positive evidence available
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # type: ignore[arg-type]  # Why: SubJobEnqueuer only None-checks the pool (the autonomous-fallback gate); the in-memory backend has no asyncpg pool.
        backend=backend,
        queues_strict=True,
    )
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await enqueuer.enqueue(ref, _Payload())
    # A refused child never entered the buffer either: the consumer's
    # Snooze/RetryAfter drain re-enqueues BUFFERED args, so a refused
    # name cannot re-enter through the drain.
    assert enqueuer.drain_for_re_enqueue() == []


async def test_sub_enqueuer_fanout_registered_queue_enqueues() -> None:
    """The fan-out green twin: a registered route flows, no refusal."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    ref = _make_ref(queue="email")
    _configure(backend, ref)
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # type: ignore[arg-type]  # Why: SubJobEnqueuer only None-checks the pool (the autonomous-fallback gate); the in-memory backend has no asyncpg pool.
        backend=backend,
        queues_strict=True,
    )
    handle = await enqueuer.enqueue(ref, _Payload())
    assert handle.row.status == "pending"


async def test_sub_enqueuer_fanout_escape() -> None:
    """The escape reaches the fan-out arm: a dynamic child queue passes
    with allow_unregistered=True."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # type: ignore[arg-type]  # Why: SubJobEnqueuer only None-checks the pool (the autonomous-fallback gate); the in-memory backend has no asyncpg pool.
        backend=backend,
        queues_strict=True,
    )
    ref = _make_ref(queue="shard_7_dyn")
    handle = await enqueuer.enqueue(ref, _Payload(), allow_unregistered=True)
    assert handle.row.status == "pending"


async def test_sub_enqueuer_without_strict_stays_silent() -> None:
    """Fail-open posture pinned: an enqueuer built without the knob
    (no strict reached it) enqueues without enforcement."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=object(),  # type: ignore[arg-type]  # Why: SubJobEnqueuer only None-checks the pool (the autonomous-fallback gate); the in-memory backend has no asyncpg pool.
        backend=backend,
    )
    ref = _make_ref(queue="emial")
    handle = await enqueuer.enqueue(ref, _Payload())
    assert handle.row.status == "pending"


# ── The cost claim, pinned (not adjectived) ───────────────────────────


async def test_strict_check_costs_zero_store_reads_on_the_hot_path() -> None:
    """The strict verdict rides the TTL snapshot, so it costs ZERO store
    reads at submit time — the note's own no-read-amplification pin,
    mirrored for the strict path. Mutation check: a strict check that
    queried the store per submit REDS here.

    Covers all three strict postures on one warm cache: the refusal (a
    typo'd name), the escape (``allow_unregistered=True`` — a flag
    check, not a config lookup), and the served call — none re-reads.
    """
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    ref = _make_ref()
    _configure(backend, ref)
    client = JobsClient(backend, settings=_strict_settings())

    await client.enqueue(ref, _Payload(), queue="email")  # cold: the one refresh

    reads = {"max_pending": 0, "queues": 0}
    inner_mp = backend.get_actor_max_pending
    inner_q = backend.get_actor_queues

    async def counting_mp() -> dict[str, int | None]:
        reads["max_pending"] += 1
        return await inner_mp()

    async def counting_q() -> dict[str, str]:
        reads["queues"] += 1
        return await inner_q()

    backend.get_actor_max_pending = counting_mp  # type: ignore[method-assign]
    backend.get_actor_queues = counting_q  # type: ignore[method-assign]

    with pytest.raises(UnknownQueueError):
        await client.enqueue(ref, _Payload(), queue="emial")
    assert reads == {"max_pending": 0, "queues": 0}, (
        "a warm-cache strict REFUSAL must not touch the store"
    )

    handle = await client.enqueue(ref, _Payload(), queue="shard_9_dyn", allow_unregistered=True)
    assert handle.row.status == "pending"
    assert reads == {"max_pending": 0, "queues": 0}, (
        "the escape is a flag check, never a per-submit re-read"
    )

    served = await client.enqueue(ref, _Payload(), queue="email")
    assert served.row.status == "pending"
    assert reads == {"max_pending": 0, "queues": 0}, (
        "a warm-cache SERVED call under strict must not touch the store"
    )


async def test_strict_fail_open_makes_no_retry_storm() -> None:
    """The fail-open arm's cost pin: with the snapshot unavailable, a
    sick backend is read AT MOST once per TTL (the refresh-failure stamp
    bounds the retry rate), single-flight, and every enqueue in the
    window still SUCCEEDS — strict must not turn a read blip into a
    failing-query storm on the hot path."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    attempts = {"mp": 0}

    class _Sick(InMemoryBackend):
        async def get_actor_max_pending(self) -> dict[str, int | None]:
            attempts["mp"] += 1
            raise RuntimeError("backend down")

    sick = _Sick.__new__(_Sick)
    sick.__dict__.update(backend.__dict__)
    client = JobsClient(sick, settings=_strict_settings())
    ref = _make_ref()

    for _ in range(5):
        handle = await client.enqueue(ref, _Payload(), queue="ghost_queue")
        assert handle.row.status == "pending", "fail-open: every enqueue succeeds"
    assert attempts["mp"] == 1, "one read attempt per TTL, not one per submit"


async def test_batch_finalizer_refuses_unknown_queue_atomic() -> None:
    """THE FINALIZER BYPASS (the red-team's blocking find): the
    finalizer's args write through their OWN sites after the per-item
    verdict loop, which iterates items only — a finalizer routed to an
    unregistered queue stored the orphan SILENTLY (no refusal, no note)
    on every batch arm. The atomic arm's pin: refusal at the door,
    nothing stored."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    served = _make_ref(queue="email")
    _configure(backend, served)
    finalizer = _make_ref(name="fin", queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch(
            [_item(served)], finalizer=EnqueueItem(actor_ref=finalizer, payload=_Payload())
        )
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row — the finalizer's orphan included.


async def test_batch_finalizer_refuses_unknown_queue_caller_connection() -> None:
    """The caller-connection write site of enqueue_batch's finalizer (the
    enqueue_with_conn arm) refuses through the SAME verdict — one seam
    covers both write sites, pinned drivable."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    served = _make_ref(queue="email")
    _configure(backend, served)
    finalizer = _make_ref(name="fin", queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch(
            [_item(served)],
            finalizer=EnqueueItem(actor_ref=finalizer, payload=_Payload()),
            connection=object(),  # type: ignore[arg-type]  # Why: the in-memory backend takes any connection stand-in; the pin drives the caller-connection arm.
        )
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_batch_finalizer_registered_queue_enqueues() -> None:
    """The green twin: a registered finalizer route flows, no refusal,
    the batch and its finalizer both land."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    served = _make_ref(queue="email")
    _configure(backend, served)
    finalizer = _make_ref(name="fin", queue="weather")
    _configure(backend, finalizer)
    handle_batch = await client.enqueue_batch(
        [_item(served)], finalizer=EnqueueItem(actor_ref=finalizer, payload=_Payload())
    )
    assert handle_batch.finalizer_handle is not None
    assert handle_batch.finalizer_handle.row.status == "pending"


async def test_streaming_finalizer_refuses_unknown_queue_atomic() -> None:
    """The streaming arm's ATOMIC path has its own finalizer write (the
    args ride the backend's atomic call) and its own warm-up seam: the
    verdict rides the warmed snapshot, before the write."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    served = _make_ref(queue="email")
    _configure(backend, served)
    finalizer = _make_ref(name="fin", queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch_streaming(
            (item for item in [_item(served)]),
            finalizer=EnqueueItem(actor_ref=finalizer, payload=_Payload()),
        )
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_streaming_finalizer_refuses_unknown_queue_caller_connection() -> None:
    """The streaming arm's chunked (caller-connection) path writes the
    finalizer FIRST, before any chunk loop refresh — the distinct seam:
    the verdict warms the snapshot itself (the single-refresh budget),
    then refuses before the insert."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
    served = _make_ref(queue="email")
    _configure(backend, served)
    finalizer = _make_ref(name="fin", queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue_batch_streaming(
            (item for item in [_item(served)]),
            finalizer=EnqueueItem(actor_ref=finalizer, payload=_Payload()),
            connection=object(),  # type: ignore[arg-type]  # Why: the in-memory backend takes any connection stand-in; the pin drives the chunked caller-connection arm.
        )
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


# ── The row-creating bypass paths, exempt by construction ─────────────


async def test_cron_fire_targets_the_stored_assignment() -> None:
    """The cron-fire bypass's pin: a fire's queue IS the stored
    ``actor_config`` assignment -- the snapshot's own source -- so the
    client strict gate never sees the cron path and nothing it enqueues
    can be orphaned by construction. Pinned end to end through the tick
    harness: the leader's configured consume set does not even contain
    the queue (the split-deployment shape), and the fire still lands on
    the stored assignment."""
    from tests.test_cron_loop import (
        _FakeCronConn,
        _make_actor_config_row,
        _make_schedule_row,
        _tick,
    )

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "unrelated", "TASKQ_QUEUES_STRICT": "true"}
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="cron_actor",
                next_fire_at=_NOW,
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="cron_actor", queue="cron_queue")],
    )
    fired = await _tick(conn, settings, backend)
    assert fired == 1
    (row,) = list(backend._jobs.values())  # type: ignore[reportPrivateUsage]  # Why: the pin reads the store the tick wrote through the raw backend arm.
    assert row.queue == "cron_queue"


# ── The worker-side boot gate (split-deployment safe) ─────────────────


def _row(actor: str, queue: str) -> Any:
    from taskq.actor_config_ops import ActorConfigRow

    return ActorConfigRow(
        actor=actor,
        max_concurrent=None,
        max_pending=None,
        queue=queue,
        result_ttl=None,
        metadata={},
        updated_at="",
    )


def test_worker_boot_gate_fails_fast_on_unrouted_queue() -> None:
    """Strict's worker-side half: a configured queue nothing routes to
    (no stored assignment, no registered literal) fails the boot, the
    config-drift catch the issue asks for. WORKER-side only: the client
    process has no actor registry to consult, and the boot gate never
    runs there."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email,ghost", "TASKQ_QUEUES_STRICT": "true"}
    )
    registry = {"actor_email": _make_ref(name="actor_email", queue="email")}
    with pytest.raises(UnknownQueueError) as excinfo:
        _fail_fast_on_unrouted_configured_queues(settings, registry, {})
    message = str(excinfo.value)
    assert "ghost" in message, message
    assert "email" in message, "the configured set is named"
    assert "TASKQ_QUEUES" in message, message
    assert excinfo.value.source == "worker_boot"


def test_worker_boot_gate_message_pluralizes() -> None:
    """One raise refuses EVERY unrouted queue, so the message reads
    right at both lengths: singular 'queue ... has ... it', plural
    'queues ... have ... them'."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email,ghost,phantom", "TASKQ_QUEUES_STRICT": "true"}
    )
    registry = {"actor_email": _make_ref(name="actor_email", queue="email")}
    with pytest.raises(UnknownQueueError) as excinfo:
        _fail_fast_on_unrouted_configured_queues(settings, registry, {})
    message = str(excinfo.value)
    assert (
        "configured queues 'ghost', 'phantom' have no registered actor routing to them" in message
    )
    assert "jobs on them would never be dispatched" in message
    # Singular stays singular.
    one = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email,ghost", "TASKQ_QUEUES_STRICT": "true"}
    )
    with pytest.raises(UnknownQueueError) as one_info:
        _fail_fast_on_unrouted_configured_queues(one, registry, {})
    assert "configured queue 'ghost' has no registered actor routing to it" in str(one_info.value)
    assert "jobs on it would never be dispatched" in str(one_info.value)


def test_worker_boot_gate_passes_when_every_configured_queue_is_routed() -> None:
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email,weather", "TASKQ_QUEUES_STRICT": "true"}
    )
    registry = {
        "actor_email": _make_ref(name="actor_email", queue="email"),
        "actor_weather": _make_ref(name="actor_weather", queue="weather"),
    }
    _fail_fast_on_unrouted_configured_queues(settings, registry, {})


def test_worker_boot_gate_off_by_default() -> None:
    """Default never mutates explicit intent: without the knob, the same
    unrouted configuration boots (the existing aggregated WARNING keeps
    owning that signal)."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "email,ghost"})
    registry = {"actor_email": _make_ref(name="actor_email", queue="email")}
    _fail_fast_on_unrouted_configured_queues(settings, registry, {})


def test_worker_boot_gate_stored_assignment_provides_coverage() -> None:
    """The stored (operator-owned) assignment is what routes cron fires
    and re-pended rows: an actor whose stored row routes a configured
    queue covers it EVEN when this process's registry is empty -- the
    rolling-deploy shape (the sibling image registered the actor; its
    row persists fleet-wide) boots."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email", "TASKQ_QUEUES_STRICT": "true"}
    )
    _fail_fast_on_unrouted_configured_queues(
        settings, {}, {"actor_email": _row("actor_email", "email")}
    )


def test_worker_boot_gate_judges_nothing_on_empty_configured_set() -> None:
    """An empty TASKQ_QUEUES is the worker-consumes-no-queues failure
    the existing warning owns; the gate stays out of it.

    TASKQ_QUEUES itself never loads empty (it defaults to ``["default"]``);
    the empty-set shape comes from the CLI's ``--queues ""`` arm, so the
    posture is exercised through a stub carrying it."""
    from taskq.worker._bootstrap import _fail_fast_on_unrouted_configured_queues

    class _EmptyQueues:
        queues: ClassVar[list[str]] = []
        queues_strict: ClassVar[bool] = True

    _fail_fast_on_unrouted_configured_queues(
        _EmptyQueues(),  # type: ignore[arg-type]  # Why: the guard's posture is what's pinned; the CLI's --queues "" arm produces this shape.
        {},
        {},
    )


# ── The asymmetry doctrine: over-rejection is strictly worse ──────────


async def test_empty_snapshot_never_refuses_first_deploy() -> None:
    """THE EMPTY-SNAPSHOT RULE (the first-deploy explosion guard): a
    stored-assignment snapshot that is EMPTY — no worker has ever
    started — carries ZERO evidence of stranding, so strict DISABLES
    itself (advisory posture, the note still fires) and the job is
    ACCEPTED. Red-first: this exact submit raised at the base before the
    rule landed."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))  # fresh store: nothing registered
    client = JobsClient(backend, settings=_strict_settings())
    ref = _make_ref()
    with structlog.testing.capture_logs() as logs:
        handle = await client.enqueue(ref, _Payload(), queue="email")
    assert handle.row.status == "pending", "the first-deploy submit is accepted"
    assert any(e.get("event") == "enqueue-unserved-queue" for e in logs), (
        "the advisory note still fires"
    )


async def test_env_declared_queue_passes_through_registration_lag() -> None:
    """THE TWO-SOURCE RULE, mid-deploy pin: a queue the process's env
    set declares but the snapshot lacks is a NEW queue mid-deploy (the
    worker boots later) — ALLOWED through the registration lag, the
    advisory note still fires. Only a queue absent from BOTH sources
    refuses."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email,billing", "TASKQ_QUEUES_STRICT": "true"}
    )
    served = _make_ref(queue="email")
    _configure(backend, served)  # the snapshot knows email, not billing
    client = JobsClient(backend, settings=settings)
    ref = _make_ref(queue="billing")
    with structlog.testing.capture_logs() as logs:
        handle = await client.enqueue(ref, _Payload())
    assert handle.row.status == "pending", "env-declared passes the lag"
    assert any(e.get("event") == "enqueue-unserved-queue" for e in logs), (
        "the advisory note still fires until the assignment registers"
    )
    # And the both-sources-miss twin still refuses.
    stray = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue(stray, _Payload())


async def test_snapshot_only_source_refusal_names_the_deploy_order() -> None:
    """The web-without-env corner, pinned: a client whose settings carry
    no queue set judges on the snapshot ALONE — the refusal message says
    exactly that, and names the operator's protection (deploy workers
    before clients for new queues)."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    served = _make_ref(queue="email")
    _configure(backend, served)
    client = JobsClient(backend, settings=_strict_settings())  # TaskQSettings: no env set
    ref = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError) as excinfo:
        await client.enqueue(ref, _Payload())
    message = str(excinfo.value)
    assert "declares no TASKQ_QUEUES set" in message, message
    assert "workers before clients" in message, message


async def test_partial_read_heals_via_the_next_refresh() -> None:
    """A failed read DISABLES the check (the pin above); the recovery
    half: once the store heals, the NEXT refresh (the TTL cadence —
    invalidate() stands in for the clock here, the same seam operator
    tooling uses) re-arms the verdict. No refusal may originate from a
    deploy/migration artifact, and none persists past the heal."""
    sick = {"down": True}

    class _Flappy(InMemoryBackend):
        async def get_actor_max_pending(self) -> dict[str, int | None]:
            if sick["down"]:
                raise RuntimeError("migration in flight")
            return await super().get_actor_max_pending()

    flappy = _Flappy(clock=FakeClock(_NOW))
    ref = _make_ref()
    _configure(flappy, ref)
    client = JobsClient(flappy, settings=_strict_settings())
    # The migration window: the read fails, strict fails OPEN.
    handle = await client.enqueue(ref, _Payload(), queue="email")
    assert handle.row.status == "pending"
    # The store heals; the next refresh (TTL) re-arms the verdict.
    sick["down"] = False
    client.invalidate_actor_capacity_cache()
    ok = await client.enqueue(ref, _Payload(), queue="email")
    assert ok.row.status == "pending"
    stray = _make_ref(queue="emial")
    with pytest.raises(UnknownQueueError):
        await client.enqueue(stray, _Payload())


def test_worker_boot_gate_flags_retired_queue_assignments_as_advisory() -> None:
    """THE RETIREMENT HALF (no silent allowance forever): stored
    assignments for actors this worker neither serves nor consumes —
    retired-actor drift — surface as ONE aggregated ADVISORY at boot,
    never a refusal (the inverse of the configured-but-unrouted gate,
    which is the refusal side)."""
    from taskq.worker._bootstrap import (
        _emit_retired_queue_assignments_advisory,
        _fail_fast_on_unrouted_configured_queues,
    )

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email", "TASKQ_QUEUES_STRICT": "true"}
    )
    registry = {"actor_email": _make_ref(name="actor_email", queue="email")}
    stored = {
        "actor_email": _row("actor_email", "email"),
        "dead_actor": _row("dead_actor", "legacy_queue"),
    }
    with structlog.testing.capture_logs() as logs:
        # The configured-side gate stays silent (email is routed); the
        # retirement advisory is the only signal, and it refuses nothing.
        _fail_fast_on_unrouted_configured_queues(settings, registry, stored)
        _emit_retired_queue_assignments_advisory(
            settings, registry, stored, structlog.get_logger("test")
        )
    assert not any(e.get("event") == "enqueue-unserved-queue" for e in logs)
    retired = [e for e in logs if e.get("event") == "retired-queue-assignments"]
    assert len(retired) == 1
    assert retired[0]["queues"] == ["legacy_queue"]
    assert retired[0]["actors"] == ["dead_actor"]
    assert "retire" in str(retired[0]["note"]) or "delete" in str(retired[0]["note"])


# ── The format tier (unconditional, knob-independent) ─────────────────


@pytest.mark.parametrize(
    "bad_name",
    [
        "bad queue",  # space: outside the charset
        "",  # empty: no first character at all
        "has:colon",  # ':' collides with the queue-cap namespace separator
        "x" * 256,  # past the 255-char btree bound
        "bad\nname",  # newline (the ^/$ trap the anchored regex excludes)
    ],
)
async def test_format_tier_refuses_bad_queue_names_even_with_strict_off(
    bad_name: str,
) -> None:
    """The FORMAT tier is unconditional — it never consults the strict
    knob or the snapshot (a queue that fails format never needs either):
    every enqueue arm builds args through ``build_enqueue_args``, whose
    charset check (the canonical ``_validate_queue_name``) runs for the
    per-call override and the actor-declared default alike. Pinned
    through the client arm with strict OFF: the refusal is a ValueError
    naming the queue, and nothing is stored."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=TaskQSettings.load_from_dict({}))
    # The ref's own queue stays valid: a bad literal is refused even
    # earlier, at decoration time (actor.py's half of the same tier).
    # This pin drives the PER-CALL OVERRIDE path — the one the QueueName
    # annotation cannot see.
    ref = _make_ref(queue="email")
    with pytest.raises(ValueError) as excinfo:
        await client.enqueue(ref, _Payload(), queue=bad_name)
    message = str(excinfo.value)
    assert "queue name" in message, message
    assert len(backend._jobs) == 0  # type: ignore[reportPrivateUsage]  # Why: the pin IS the absence of a stored row.


async def test_format_error_names_the_queue_the_rule_and_the_shape() -> None:
    """River-grade error content: the offending queue, the violated rule
    (which character lost, or the length bound), and the expected shape."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=TaskQSettings.load_from_dict({}))
    ref = _make_ref()
    with pytest.raises(ValueError) as excinfo:
        await client.enqueue(ref, _Payload(), queue="bad:colon")
    message = str(excinfo.value)
    assert "bad:colon" in message, "the offending queue is named"
    assert "letters" in message or "charset" in message or ":" in message, (
        "the violated rule is named"
    )
    assert ":" in message, "the expected shape (why ':' is excluded) is stated"


# ── The celery guard: the off->on transition breaks no submit shape ───


async def test_strict_on_zero_config_default_queue_still_works() -> None:
    """Celery #6692's guard, pinned: strict ON with ZERO config (no
    TASKQ_QUEUES — the settings default `["default"]`, the default
    queue, a bare submit) still WORKS, in both deploy states: the fresh
    store (empty snapshot -> the asymmetry rule allows) and the
    registered fleet (the assignment routes). The off->on transition
    must not break any existing submit shape."""
    # Zero-config strict: no TASKQ_QUEUES, only the knob.
    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES_STRICT": "true"})
    assert settings.queues == ["default"], "the zero-config default set"
    assert settings.queues_strict is True

    # The first-deploy state: fresh store, no worker ever started.
    fresh = InMemoryBackend(clock=FakeClock(_NOW))
    fresh_client = JobsClient(fresh, settings=settings)
    ref = _make_ref(queue="default")
    handle = await fresh_client.enqueue(ref, _Payload())
    assert handle.row.status == "pending", "empty snapshot never refuses"

    # The registered fleet: the assignment routes, the submit flows.
    registered = InMemoryBackend(clock=FakeClock(_NOW))
    _configure(registered, ref)
    registered_client = JobsClient(registered, settings=settings)
    again = await registered_client.enqueue(ref, _Payload())
    assert again.row.status == "pending"

    # The mid-deploy shape (env declares default, snapshot non-empty but
    # lacking it): the two-source rule allows.
    partial = InMemoryBackend(clock=FakeClock(_NOW))
    other = _make_ref(name="other", queue="email")
    _configure(partial, other)
    partial_client = JobsClient(partial, settings=settings)
    mid = await partial_client.enqueue(ref, _Payload())
    assert mid.row.status == "pending", "env-declared passes the registration lag"


# ── The settings knob ─────────────────────────────────────────────────


def test_knob_defaults_off_and_lives_on_the_base_settings() -> None:
    """The knob follows the settings pattern: a dotenvmodel field on the
    BASE class (the client process must be able to load it -- the
    split-deployment constraint), off by default, inherited by the
    worker settings beside TASKQ_QUEUES."""
    defaults = TaskQSettings.load_from_dict({})
    assert defaults.queues_strict is False
    on = _strict_settings()
    assert on.queues_strict is True
    worker = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "email,ghost", "TASKQ_QUEUES_STRICT": "true"}
    )
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
    client = JobsClient(jobs_app.backend, settings=_strict_settings())
    ref = _make_ref()
    with pytest.raises(UnknownQueueError):
        await client.enqueue(ref, _Payload(), queue="emial")
