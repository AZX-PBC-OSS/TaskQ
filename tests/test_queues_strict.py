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
    at the submit site and the row is NOT stored."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
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
    from taskq.batch import EnqueueItem

    return EnqueueItem(actor_ref=ref, payload=_Payload())


async def test_batch_arm_refuses_unknown_queue() -> None:
    """The batch arm speaks the same contract: an item whose ref routes
    to an unregistered queue raises before any row is written."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=_strict_settings())
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
    """Fan-out children (``ctx.jobs.enqueue`` inside an actor body) ride
    the same seam: a child routed to an unregistered queue raises in the
    actor body instead of storing an orphan."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
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
