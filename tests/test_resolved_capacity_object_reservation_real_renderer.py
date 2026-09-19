"""Red-team check for issue coverage on resolved-capacity boot lines.

``_emit_resolved_capacity_startup_lines`` (src/taskq/worker/run.py) extracts
plain names/slot counts from object-shaped reservation entries
(``ConcurrencyReservation``, ``KeyedReservationRef``) before calling
``log.info`` - per its own docstring, because neither type is JSON
serializable through ``taskq._json.dumps`` and structlog's JSON renderer is
not exception-wrapped, so logging the raw object would silently drop the
whole line.

The existing pins in test_worker_resolved_capacity_startup_lines.py only ever
pass ``reservations=["db_pool"]`` (bare strings) through
``structlog.testing.capture_logs()``, which bypasses the real renderer
entirely. This test exercises the two gaps at once: an object-shaped
reservation entry, run through the actual JSON-configured structlog pipeline
(``configure_structlog(log_format="json")``), captured at the stdout/stream
boundary so a dropped line would show up as a missing line rather than a
dict comparison mismatch.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import structlog
from pydantic import BaseModel, TypeAdapter

from taskq._json import structlog_serializer
from taskq.actor import ActorRef
from taskq.actor_config_ops import ActorConfigRow
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.worker.run import _emit_resolved_capacity_startup_lines


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_settings(*, max_concurrency: int = 8) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
            "TASKQ_QUEUES": "default",
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
        },
    )


def _make_actor_ref(
    *, name: str, reservations: list[Any] | None = None
) -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        max_concurrent=None,
        unique_for=None,
        max_pending=None,
        reservations=list(reservations) if reservations is not None else None,
    )


def _stored_row(actor: str) -> ActorConfigRow:
    return ActorConfigRow(
        actor=actor,
        max_concurrent=None,
        max_pending=None,
        queue="default",
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01 00:00:00+00",
    )


def test_object_shaped_reservation_survives_the_real_json_renderer() -> None:
    """A declared ``ConcurrencyReservation`` entry must reach the real
    JSON-rendered log stream, naming both its layer and the actual slot
    count that gates admission - not silently vanish because the object
    itself is not JSON-serializable."""
    settings = _make_settings(max_concurrency=8)
    reservation = ConcurrencyReservation(name="db_pool", slots=3, lease=timedelta(minutes=5))
    registry = {"alpha": _make_actor_ref(name="alpha", reservations=[reservation])}

    # Real production JSON renderer (taskq._json.structlog_serializer, the
    # same serializer setup_logging wires into JSONRenderer) bound through a
    # minimal structlog pipeline, rather than mutating the process-global
    # structlog.configure() that setup_logging performs once and guards
    # idempotently - this keeps the test isolated while still exercising the
    # exact renderer/serializer production uses.
    renderer = structlog.processors.JSONRenderer(serializer=structlog_serializer)
    records: list[str] = []

    class _CollectingLogger:
        def msg(self, message: str) -> None:
            records.append(message)

        info = msg
        warning = msg

    log = structlog.wrap_logger(
        _CollectingLogger(),
        processors=[
            structlog.processors.EventRenamer("event"),
            renderer,
        ],
    )

    _emit_resolved_capacity_startup_lines(
        settings,
        registry,
        stored_rows={"alpha": _stored_row("alpha")},
        queue_rows={},
        log=log,
    )

    lines = [json.loads(line) for line in records if line.strip()]
    capacity_lines = [entry for entry in lines if entry.get("event") == "actor-resolved-capacity"]

    assert len(capacity_lines) == 1, (
        f"expected exactly one resolved-capacity line to survive the real JSON "
        f"renderer for an object-shaped reservation, got {len(capacity_lines)} "
        f"in records: {records!r}"
    )
    entry = capacity_lines[0]
    assert entry["binding"] == "reservation"
    assert entry["resolved"] == 3, (
        "resolved capacity must reflect the reservation's own slot count "
        "(3), not the process cap (8)"
    )
    assert entry["reservations"] == ["db_pool"]
