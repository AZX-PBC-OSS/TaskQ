"""End-to-end attribution of event-loop stalls to the actor causing them.

Both classifier shapes run in-process against the REAL event loop on the
main thread, with the real ``LoopLagWatchdog`` thread sampling it:

- ``blocking_call``: an actor whose body calls ``time.sleep`` releases the
  GIL, so the watchdog thread keeps ticking (its own waits stay on time)
  while the loop cannot schedule.
- ``gil_held``: an actor whose body parses a large document with orjson
  holds the GIL for the whole parse, starving even the watchdog thread's
  wakeups (the second classifier signal).

The asserts are the operator-facing contract: the warn carries the actor
name, the kind, a file:line:function frame, the job id when exactly one
running job matches, and the counter increments with the right labels.
"""

import asyncio
import time
from typing import Any

import pytest
import structlog
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from pydantic import BaseModel

orjson = pytest.importorskip("orjson")

import taskq.obs as obs_mod  # noqa: E402  # Why: importorskip guard must precede.
import taskq.obs._otel as otel_mod  # noqa: E402
from taskq.actor import actor  # noqa: E402
from taskq.testing.otel import counter_data_points  # noqa: E402
from taskq.worker._stall_tally import StallAttributionTally  # noqa: E402
from taskq.worker._watchdog import LoopLagWatchdog, LoopLiveness  # noqa: E402


class _Payload(BaseModel):
    value: int = 0


# A document sized so one orjson parse holds the GIL for far longer than
# the warn budget (about 60ms per parse against a 50ms warn budget): an
# unmistakable hold, not scheduler noise. Built once at import: building
# it inside the probe would itself stall the loop with pure-Python work
# and race the classifier before the parse ever ran.
_GIL_DOCUMENT = b"[" + b",".join(b"1.2345678901234" for _ in range(3_000_000)) + b"]"


@actor(name="stall_blocking_probe", queue="default")
async def _blocking_probe(payload: _Payload) -> None:
    time.sleep(0.5)  # noqa: ASYNC251  # Why: blocking the loop IS the scenario under test.


@actor(name="stall_gil_probe", queue="default")
async def _gil_probe(payload: _Payload) -> None:
    orjson.loads(_GIL_DOCUMENT)
    orjson.loads(_GIL_DOCUMENT)
    orjson.loads(_GIL_DOCUMENT)
    # Keep the loop blocked after the parses: the watchdog thread, starved
    # since the first parse began, only gets to poll once the GIL frees,
    # and this keeps the actor's own frame on the stack it samples there
    # instead of racing the beat that lands when the probe returns.
    time.sleep(0.2)  # noqa: ASYNC251  # Why: as above.


_PROBES = {ref.name: ref for ref in (_blocking_probe, _gil_probe)}


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # pyright: ignore[reportPrivateUsage]  # Why: mirrors tests/test_obs_job_outcome_counters.py's otel_reader fixture.
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: meter)
    otel_mod.set_otel_enabled(True)
    return reader


def _points(reader: InMemoryMetricReader, name: str) -> list[tuple[int, dict[str, object]]]:
    return [(int(p.value), dict(p.attributes or {})) for p in counter_data_points(reader, name)]


async def _run_probe(
    probe_name: str,
    *,
    poll_interval: float = 0.02,
    warn_budget: float = 0.05,
    running_jobs: list[tuple[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], StallAttributionTally]:
    """Run one probe actor on the real loop with the real watchdog thread.

    The warn budget sits above the poll interval so a single jittered
    poll can never warn, and the poll interval sits well above OS
    scheduler noise so the blocking shape cannot misclassify as GIL
    pressure (a 20ms wait would have to overshoot a further 20ms).
    """
    tally = StallAttributionTally()
    code_map = {id(ref.fn.__code__): ref.name for ref in _PROBES.values()}
    watchdog = LoopLagWatchdog(
        asyncio.get_running_loop(),
        LoopLiveness(),
        budget=100.0,
        warn_budget=warn_budget,
        startup_grace=0.0,
        poll_interval=poll_interval,
        actor_code_names=code_map,
        stall_tally=tally,
        list_running_jobs=lambda: running_jobs or [],
    )
    watchdog.start()
    try:
        with structlog.testing.capture_logs() as logs:
            await _PROBES[probe_name](_Payload())
    finally:
        watchdog.stop()
    events: list[dict[str, Any]] = [
        dict(e) for e in logs if e["event"] == "event-loop-stall-attributed"
    ]
    return events, tally


async def test_blocking_call_stall_is_attributed_to_the_actor() -> None:
    events, tally = await _run_probe(
        "stall_blocking_probe",
        running_jobs=[("stall_blocking_probe", "job-block-1")],
    )
    assert events, "the warn tier must emit an attributed stall warning"
    event = events[0]
    assert event["actor"] == "stall_blocking_probe"
    assert event["kind"] == "blocking_call"
    assert event["job_id"] == "job-block-1"
    assert event["frame"] is not None and ":_blocking_probe" in event["frame"]
    assert event["samples"] >= 1
    assert len(event["stack_frames"]) <= 8
    assert "asyncio.to_thread" in event["remedy"]
    assert tally.snapshot() == {"stall_blocking_probe": {"blocking_call": 1}}


async def test_gil_held_stall_is_attributed_to_the_actor() -> None:
    events, tally = await _run_probe("stall_gil_probe")
    assert events, "the warn tier must emit an attributed stall warning"
    event = events[0]
    assert event["actor"] == "stall_gil_probe"
    assert event["kind"] == "gil_held"
    assert event["frame"] is not None and ":_gil_probe" in event["frame"]
    assert "chunk" in event["remedy"]
    assert tally.snapshot() == {"stall_gil_probe": {"gil_held": 1}}


async def test_blocking_stall_increments_the_counter_with_its_labels(
    otel_reader: InMemoryMetricReader,
) -> None:
    await _run_probe("stall_blocking_probe")
    matching = [
        count
        for count, attrs in _points(otel_reader, "taskq.worker.loop_stall_attributions")
        if attrs.get("actor") == "stall_blocking_probe" and attrs.get("kind") == "blocking_call"
    ]
    assert sum(matching) >= 1


async def test_gil_held_stall_increments_the_counter_with_its_labels(
    otel_reader: InMemoryMetricReader,
) -> None:
    await _run_probe("stall_gil_probe")
    matching = [
        count
        for count, attrs in _points(otel_reader, "taskq.worker.loop_stall_attributions")
        if attrs.get("actor") == "stall_gil_probe" and attrs.get("kind") == "gil_held"
    ]
    assert sum(matching) >= 1


def test_unattributed_stall_carries_the_overflow_label(
    otel_reader: InMemoryMetricReader,
) -> None:
    """A stall whose sampled stack names no registered actor still counts,
    under the same overflow label every actor-bounded counter uses."""
    obs_mod.record_loop_stall_attribution(None, kind="gil_held")
    assert _points(otel_reader, "taskq.worker.loop_stall_attributions") == [
        (1, {"actor": otel_mod._ACTOR_LABEL_OVERFLOW, "kind": "gil_held"})  # pyright: ignore[reportPrivateUsage]  # Why: the overflow label is the counter's own closed-set contract, asserted where it is consumed.
    ]
