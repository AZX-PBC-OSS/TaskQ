"""Unit tests for _emit_queue_subscription_warnings.

Pure-Python tests — no PG required. The helper is a synchronous function
of (``WorkerSettings.queues``, ``actor_registry``) whose only side effect
is the warning log, so stranding visibility can be exercised directly
without standing up a whole worker — same pattern as
test_worker_startup_warnings.py.

Covers: the warning fires exactly once when a registered actor targets a
queue this worker does not consume; it stays silent when every actor's
queue is consumed (including multi-queue workers); the payload carries the
actionable content (which actors, which queues, what this worker
consumes, and the only-if-no-other-worker clarification); and an empty
registry has nothing to warn about.
"""

import structlog.testing
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.spy import WarningSpy
from taskq.worker.run import _emit_queue_subscription_warnings


def _make_settings(queues: str) -> WorkerSettings:
    """WorkerSettings with only the queue subscription configured."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
            "TASKQ_QUEUES": queues,
        }
    )


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(*, name: str, queue: str) -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

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


async def test_actor_on_unconsumed_queue_emits_warning() -> None:
    """Worker consumes only 'default'; actor registered on 'cron' → one
    warning (the stranding failure mode: jobs pile up pending forever)."""
    settings = _make_settings("default")
    actor_registry = {"nightly": _make_actor_ref(name="nightly", queue="cron")}
    spy = WarningSpy()

    _emit_queue_subscription_warnings(settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_all_actors_on_consumed_queues_emits_no_warning() -> None:
    """Worker consumes both queues the actors target → silent (legitimate
    topology, nothing stranded here)."""
    settings = _make_settings("default,cron")
    actor_registry = {
        "inline": _make_actor_ref(name="inline", queue="default"),
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
    }
    spy = WarningSpy()

    _emit_queue_subscription_warnings(settings, actor_registry, spy)

    assert spy.warning_count == 0


async def test_default_topology_emits_no_warning() -> None:
    """The everything-on-default deployment stays silent."""
    settings = _make_settings("default")
    actor_registry = {"plain": _make_actor_ref(name="plain", queue="default")}
    spy = WarningSpy()

    _emit_queue_subscription_warnings(settings, actor_registry, spy)

    assert spy.warning_count == 0


async def test_empty_registry_emits_no_warning() -> None:
    """No actors registered → no subject to warn about."""
    settings = _make_settings("default")
    actor_registry: dict[str, ActorRef[_Payload, _Result]] = {}
    spy = WarningSpy()

    _emit_queue_subscription_warnings(settings, actor_registry, spy)

    assert spy.warning_count == 0


async def test_mixed_registry_emits_single_warning_listing_every_stranded_actor() -> None:
    """One consumed actor + two unconsumed actors → exactly one warning,
    naming both stranded actors, not one log line per actor."""
    settings = _make_settings("default")
    actor_registry = {
        "inline": _make_actor_ref(name="inline", queue="default"),
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
        "mailer": _make_actor_ref(name="mailer", queue="email"),
    }
    spy = WarningSpy()

    _emit_queue_subscription_warnings(settings, actor_registry, spy)

    assert spy.warning_count == 1


async def test_warning_payload_names_actors_queues_and_worker_subscription() -> None:
    """The payload must be actionable: which actors sit on which unconsumed
    queues, what this worker does consume, and the clarification that the
    topology is only broken when no other worker consumes those queues."""
    settings = _make_settings("default")
    actor_registry = {
        "nightly": _make_actor_ref(name="nightly", queue="cron"),
        "mailer": _make_actor_ref(name="mailer", queue="email"),
    }

    with structlog.testing.capture_logs() as logs:
        _emit_queue_subscription_warnings(settings, actor_registry, structlog.get_logger())

    entry = next(log for log in logs if log["event"] == "actors-on-unconsumed-queues")
    assert entry["log_level"] == "warning"
    assert entry["actors"] == {"nightly": "cron", "mailer": "email"}
    assert entry["worker_queues"] == ["default"]
    # The load-bearing clarification: split-queue topologies are legitimate
    # when another worker consumes the queue.
    assert "no other worker" in entry["note"]
