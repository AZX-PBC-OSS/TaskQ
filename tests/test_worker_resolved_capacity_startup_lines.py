"""Worker boot must publish the concurrency each registered actor actually gets.

Capacity in TaskQ is resolved from four independent places that no single
configuration surface shows together: the worker's own ``max_concurrency``
(the process cap), the actor's stored ``actor_config.max_concurrent``,
the ``queues`` row's ``max_concurrent`` for the queue the actor is
assigned to, and the actor's declared reservations / ``singleton=True``.
Whichever of those is smallest is the one that binds, and an operator who
raises the wrong one sees no change at all — the classic "I bumped the
cap and nothing happened" report. The boot line is the answer: one
``actor-resolved-capacity`` record per registered actor carrying both the
number that binds and the layer that produced it, so the fix is a single
grep away rather than a four-surface archaeology exercise.

A second, narrower line rides the same pass: when the stored
``actor_config`` row's capacity disagrees with the value declared in the
``@actor(...)`` literal, the stored value silently wins (the startup
UPSERT deliberately leaves the capacity columns alone once a row exists).
That divergence is legitimate — stored capacity is operator-owned — but
it is exactly the state where a code change appears to be ignored, so
boot says so out loud instead of leaving the operator to infer it.

Neither line ever refuses boot: a worker that can do work must start.
These are observability, and observability is what stands in for the
refusals TaskQ deliberately does not issue here.

Pure-Python unit tests — no PG required. Lines are asserted as actually
emitted (event, level, fields) via ``structlog.testing.capture_logs``,
following ``tests/test_worker_unconsumed_queue_warning.py``.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import structlog.testing
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.actor_config_ops import ActorConfigRow
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.worker.queue_ops import QueueRow
from taskq.worker.run import _startup_log

_CAPACITY_EVENT = "actor-resolved-capacity"
_DIVERGENCE_EVENT = "actor-config-capacity-divergence"


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _emit(
    settings: WorkerSettings,
    registry: Mapping[str, ActorRef[Any, Any]],
    *,
    stored_rows: Mapping[str, ActorConfigRow] | None = None,
    queue_rows: Mapping[str, QueueRow] | None = None,
) -> None:
    """Call the boot-time capacity emitter through its public seam.

    Resolved through ``taskq.worker.run`` at call time rather than at
    import, so the absence of the emitter fails each pin on its own terms
    instead of aborting collection for the whole module.
    """
    import taskq.worker.run as run_mod

    emitter = getattr(run_mod, "_emit_resolved_capacity_startup_lines", None)
    assert emitter is not None, (
        "taskq.worker.run must expose _emit_resolved_capacity_startup_lines: "
        "worker boot has to publish the concurrency each registered actor "
        "actually resolves to, and the layer that produced it"
    )
    emitter(
        settings,
        registry,
        stored_rows=dict(stored_rows or {}),
        queue_rows=dict(queue_rows or {}),
        log=_startup_log,
    )


def _make_settings(*, queues_csv: str = "default", max_concurrency: int = 8) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@localhost:5432/taskq",
            "TASKQ_QUEUES": queues_csv,
            "TASKQ_MAX_CONCURRENCY": str(max_concurrency),
        },
    )


def _make_actor_ref(
    *,
    name: str,
    queue: str = "default",
    max_concurrent: int | None = None,
    singleton: bool = False,
    reservations: list[str] | None = None,
) -> ActorRef[_Payload, _Result]:
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
        singleton=singleton,
        max_concurrent=max_concurrent,
        unique_for=None,
        max_pending=None,
        reservations=list(reservations) if reservations is not None else None,
    )


def _stored_row(
    actor: str, *, max_concurrent: int | None, queue: str = "default"
) -> ActorConfigRow:
    return ActorConfigRow(
        actor=actor,
        max_concurrent=max_concurrent,
        max_pending=None,
        queue=queue,
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01 00:00:00+00",
    )


def _capacity_lines(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [entry for entry in logs if entry["event"] == _CAPACITY_EVENT]


def test_one_resolved_capacity_line_per_registered_actor() -> None:
    """The contract is per-actor completeness: an operator reading a boot
    log must find every registered actor's number without knowing in
    advance which ones are interesting. A line emitted only for capped
    actors leaves the uncapped ones looking unregistered."""
    settings = _make_settings()
    registry = {
        "alpha": _make_actor_ref(name="alpha"),
        "beta": _make_actor_ref(name="beta", max_concurrent=2),
        "gamma": _make_actor_ref(name="gamma", singleton=True),
    }

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry)

    lines = _capacity_lines(logs)
    assert len(lines) == 3, f"one line per registered actor expected: {logs}"
    assert {entry["actor"] for entry in lines} == {"alpha", "beta", "gamma"}


def test_process_cap_named_when_it_is_the_smallest_layer() -> None:
    """With no actor, queue, or reservation limit below it, the worker's own
    ``max_concurrency`` is what an actor actually gets. Naming it matters
    because it is the one layer that is per-process rather than
    fleet-wide: raising the stored actor cap will not move it."""
    settings = _make_settings(max_concurrency=4)
    registry = {"alpha": _make_actor_ref(name="alpha")}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"alpha": _stored_row("alpha", max_concurrent=None)})

    entry = next(iter(_capacity_lines(logs)))
    assert entry["log_level"] == "info", "a resolved capacity is normal state, not a warning"
    assert entry["binding"] == "process"
    assert entry["resolved"] == 4


def test_actor_cap_named_when_the_stored_row_binds() -> None:
    """The stored ``actor_config.max_concurrent`` is the fleet-wide cap and
    the value the dispatch gate actually reads — when it is below the
    process cap it is the binding layer, and the stored row (not the code
    literal) is the number that must be reported."""
    settings = _make_settings(max_concurrency=8)
    registry = {"alpha": _make_actor_ref(name="alpha", max_concurrent=8)}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"alpha": _stored_row("alpha", max_concurrent=3)})

    entry = next(iter(_capacity_lines(logs)))
    assert entry["binding"] == "actor"
    assert entry["resolved"] == 3


def test_queue_cap_named_when_the_queues_row_binds() -> None:
    """A ``queues`` row cap is shared by every actor assigned to that queue,
    so it can throttle an actor whose own cap was never touched — the
    hardest of the four layers to discover by hand, because nothing about
    the actor mentions it."""
    settings = _make_settings(max_concurrency=8)
    registry = {"alpha": _make_actor_ref(name="alpha", queue="batch")}

    with structlog.testing.capture_logs() as logs:
        _emit(
            settings,
            registry,
            stored_rows={"alpha": _stored_row("alpha", max_concurrent=6, queue="batch")},
            queue_rows={"batch": QueueRow(name="batch", mode="strict_fifo", max_concurrent=2)},
        )

    entry = next(iter(_capacity_lines(logs)))
    assert entry["binding"] == "queue"
    assert entry["resolved"] == 2


def test_singleton_named_as_the_binding_layer() -> None:
    """``singleton=True`` pins the actor to one in-flight job regardless of
    every numeric cap above it. Reporting "1 (actor)" would send an
    operator to raise a stored cap that cannot possibly help."""
    settings = _make_settings(max_concurrency=8)
    registry = {"solo": _make_actor_ref(name="solo", singleton=True, max_concurrent=5)}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"solo": _stored_row("solo", max_concurrent=5)})

    entry = next(iter(_capacity_lines(logs)))
    assert entry["binding"] == "singleton"
    assert entry["resolved"] == 1


def test_reservation_named_when_a_declared_reservation_binds() -> None:
    """A declared concurrency reservation caps the actor from outside the
    actor_config/queues tables entirely; without it named, its throttling
    is invisible to anyone reading the database."""
    settings = _make_settings(max_concurrency=8)
    registry = {"alpha": _make_actor_ref(name="alpha", reservations=["db_pool"])}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"alpha": _stored_row("alpha", max_concurrent=None)})

    entry = next(iter(_capacity_lines(logs)))
    assert entry["binding"] == "reservation", (
        "a declared reservation gates admission below every numeric cap and must be named"
    )
    assert entry["reservations"] == ["db_pool"]


def test_drain_mode_actor_cap_of_zero_is_labelled_not_reported_as_uncapped() -> None:
    """A stored ``max_concurrent=0`` is deliberate drain mode, and zero is
    the value most likely to be mistaken for "unset" by a reader (and by
    a falsy-check implementation). It must resolve to 0 with the actor
    layer binding — never silently fall through to the process cap."""
    settings = _make_settings(max_concurrency=8)
    registry = {"alpha": _make_actor_ref(name="alpha")}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"alpha": _stored_row("alpha", max_concurrent=0)})

    entry = next(iter(_capacity_lines(logs)))
    assert entry["resolved"] == 0
    assert entry["binding"] == "actor"
    assert entry["drain_mode"] is True, (
        "drain mode must be labelled explicitly — an actor that dispatches nothing "
        "looks identical to a broken one otherwise"
    )


def test_actor_with_no_stored_row_reports_that_it_does_not_dispatch() -> None:
    """The dispatch capacity gate joins ``actor_config``, so an actor whose
    row has never been seeded dispatches nothing at all — effective zero,
    not the code literal. A boot line that reported the literal here
    would actively mislead."""
    settings = _make_settings(max_concurrency=8)
    registry = {"fresh": _make_actor_ref(name="fresh", max_concurrent=5)}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry)

    entry = next(iter(_capacity_lines(logs)))
    assert entry["resolved"] == 0
    assert entry["binding"] == "no-stored-row"


def test_stored_null_capacity_is_labelled_uncapped_rather_than_blank() -> None:
    """``NULL`` in the stored column means "no actor-level cap", which is a
    real, intentional configuration — it must read as ``uncapped`` in the
    line rather than as an empty field an operator reads as missing
    data."""
    settings = _make_settings(max_concurrency=8)
    registry = {"alpha": _make_actor_ref(name="alpha")}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"alpha": _stored_row("alpha", max_concurrent=None)})

    entry = next(iter(_capacity_lines(logs)))
    assert entry["actor_cap"] == "uncapped"
    assert entry["binding"] == "process", "with no actor cap the process cap is what binds"


def test_stored_capacity_below_declared_literal_logs_a_divergence_warning() -> None:
    """The startup UPSERT leaves capacity columns alone once a row exists, so
    a deployed literal change is silently ignored. That is by design —
    stored capacity is operator-owned — but "my change did nothing" is
    the most expensive way for an operator to discover it, so the
    divergence is stated at boot with both values."""
    settings = _make_settings(max_concurrency=8)
    registry = {"alpha": _make_actor_ref(name="alpha", max_concurrent=6)}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"alpha": _stored_row("alpha", max_concurrent=1)})

    matches = [entry for entry in logs if entry["event"] == _DIVERGENCE_EVENT]
    assert len(matches) == 1, f"exactly one divergence line expected: {logs}"
    entry = matches[0]
    assert entry["log_level"] == "warning"
    assert entry["actor"] == "alpha"
    assert entry["declared"] == 6
    assert entry["stored"] == 1


def test_matching_stored_and_declared_capacity_logs_no_divergence() -> None:
    """Control: the divergence line must stay silent when the two agree, or
    every correctly configured worker emits it and it stops being read."""
    settings = _make_settings(max_concurrency=8)
    registry = {"alpha": _make_actor_ref(name="alpha", max_concurrent=3)}

    with structlog.testing.capture_logs() as logs:
        _emit(settings, registry, stored_rows={"alpha": _stored_row("alpha", max_concurrent=3)})

    assert [entry for entry in logs if entry["event"] == _DIVERGENCE_EVENT] == []


def test_capacity_lines_never_raise_and_never_refuse_boot() -> None:
    """The governing rule: a worker that can do work must start. Every one of
    these conditions — drain mode, a missing row, a queue with no row at
    all, divergence — is diagnosable-but-workable, so the pass returns
    normally on all of them at once rather than raising."""
    settings = _make_settings(queues_csv="default,batch", max_concurrency=8)
    registry = {
        "drained": _make_actor_ref(name="drained"),
        "unseeded": _make_actor_ref(name="unseeded", max_concurrent=4),
        "diverged": _make_actor_ref(name="diverged", max_concurrent=9, queue="batch"),
    }

    with structlog.testing.capture_logs() as logs:
        _emit(
            settings,
            registry,
            stored_rows={
                "drained": _stored_row("drained", max_concurrent=0),
                "diverged": _stored_row("diverged", max_concurrent=1, queue="batch"),
            },
        )

    assert len(_capacity_lines(logs)) == 3
    assert not any(entry["log_level"] == "error" for entry in logs), (
        "no diagnosable-but-workable capacity state may be reported as an error"
    )


def test_main_wires_the_capacity_lines_after_config_sync() -> None:
    """Structural wiring pin: helper-level tests stay green when the
    production call in ``_main`` is deleted, so the call site gets its own
    guard. It must run after ``sync_actor_config``, because only then is
    every registered actor guaranteed a stored row to resolve against —
    reporting capacity from a pre-sync read would label freshly deployed
    actors as never-dispatching on their very first boot.

    AST-based rather than source-text matching, so reformatting cannot
    break it.
    """
    import ast

    import taskq.worker._bootstrap as bootstrap_mod

    source = ast.parse(Path(bootstrap_mod.__file__).read_text())

    main_fn = next(
        (
            node
            for node in ast.walk(source)
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_main"
        ),
        None,
    )
    assert main_fn is not None, "_main must exist in taskq.worker._bootstrap"

    def _call_name(call: ast.Call) -> str | None:
        func = call.func
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return None

    emit_calls: list[ast.Call] = []
    sync_calls: list[ast.Call] = []
    for node in ast.walk(main_fn):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        if name == "_emit_resolved_capacity_startup_lines":
            emit_calls.append(node)
        elif name == "sync_actor_config":
            sync_calls.append(node)

    assert len(emit_calls) == 1, (
        "expected exactly one _emit_resolved_capacity_startup_lines call in _main, "
        f"found {len(emit_calls)} at lines {[c.lineno for c in emit_calls]}"
    )
    assert sync_calls, "_main must call sync_actor_config"
    assert emit_calls[0].lineno > min(call.lineno for call in sync_calls), (
        "the capacity lines must be emitted AFTER sync_actor_config so every "
        "registered actor has a stored row to resolve against"
    )
