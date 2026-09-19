"""The actor-facing surface of the in-memory runner's stub context.

``_StubContext`` is a declared minimal subset of the production
``taskq.context.JobContext`` - the members actors read under
``run_until_drained``. Two contracts are pinned here:

* ``span`` - the documented trace-correlation field
  (``docs/guides/observability.md``: "``ctx.span`` is the live consumer
  span, or ``None`` when OTel is disabled"). The in-memory runner is
  uninstrumented, so the parity-correct reading is ``None`` - exactly
  what a production worker without a tracer hands the actor - and the
  read itself must not fail. This is the runner half of the stub-context
  parity contract;
  the mirror half is guarded by
  ``tests/test_job_context_mirror_field_parity.py``, which excludes
  ``_StubContext`` from its field walk (declared minimal subset), so
  the runner half is behaviour-pinned here instead.

* The documented method surface - ``await ctx.progress(...)``,
  ``ctx.check_cancelled()``, and ``ctx.should_abort()``
  (``docs/guides/progress.md`` teaches progress reporting as a headline
  actor feature, with a no-Redis PG fallback, so it is core, not
  optional; ``should_abort`` is the documented cooperative-cancellation
  check sync actors poll). Production carries all four actor-facing
  members (``cancellation_requested``, ``check_cancelled``,
  ``should_abort``, ``progress`` - ``src/taskq/context.py:82-103``),
  and the runner's stub context now carries them too (the mirrored
  method surface): cancellation checks read the runner's cancel event,
  and progress reports land observably on the context's
  ``progress_reports`` with a strictly monotone ``seq`` - the runner
  has no Redis/Postgres wiring, so recording, not publishing, is the
  faithful harness half of the contract.
"""

import asyncio
from datetime import UTC, datetime

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_stub_context_span_read_matches_the_documented_disabled_value() -> None:
    """An actor reading ``ctx.span`` through ``run_until_drained``
    observes ``None`` - the documented OTel-disabled value - and the job
    succeeds. A failing read would be the stub-context drift class on
    the runner path: an actor written to the documented contract
    breaking only under the test backend."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    observed: list[object] = []

    def reader(payload: object, ctx: object) -> object:
        observed.append(ctx.span)  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is that the read works and yields the documented disabled value.
        return {"ok": True}

    backend.register_stub("reader", reader)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="reader",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded", (
        "a stub reading the documented ctx.span field must succeed; "
        f"got status={row.status} error_class={row.error_class}"
    )
    assert observed == [None], (
        "the runner hands the actor the documented OTel-disabled value "
        f"(None, as an uninstrumented production worker would); got {observed}"
    )


async def test_documented_method_surface_is_exercisable_through_the_runner() -> None:
    """An actor calling the documented ``await ctx.progress(...)``,
    ``ctx.check_cancelled()``, and ``ctx.should_abort()`` through
    ``run_until_drained`` succeeds, and the progress report lands
    observably on the context - recorded with a strictly monotone
    ``seq``, the faithful harness half of a contract whose production
    half publishes."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    contexts: list[object] = []

    async def reporter(payload: object, ctx: object) -> object:
        contexts.append(ctx)
        await ctx.progress(step=1, percent=50.0)  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is that the documented call works and lands observably.
        await ctx.progress(step=2)  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; a second report pins the strictly-monotone seq.
        ctx.check_cancelled()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is the documented raising check.
        assert not ctx.should_abort(), "no cancellation was requested"  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the sync-actor cooperative-cancellation check is part of the documented surface.
        return {"ok": True}

    backend.register_stub("reporter", reporter)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="reporter",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded", (
        "a stub using the documented method surface must succeed; "
        f"got status={row.status} error_class={row.error_class}"
    )
    reports = contexts[0].progress_reports  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is that the report is observably recorded on the context.
    assert reports == [
        {"seq": 1, "step": 1, "percent": 50.0, "detail": None, "data": None},
        {"seq": 2, "step": 2, "percent": None, "detail": None, "data": None},
    ], f"each report must land observably with a strictly monotone seq; got {reports}"


async def test_stub_context_cancellation_methods_observe_a_requested_cancel() -> None:
    """The raising half of the cancellation contract: with the job's
    cancel event already set, the stub observes
    ``cancellation_requested`` and ``should_abort()`` as True and
    ``check_cancelled()`` raises :class:`asyncio.CancelledError` - not
    only the quiet fresh-dispatch readings the exercisability pin
    covers."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    observed: dict[str, object] = {}

    def probe(payload: object, ctx: object) -> object:
        observed["cancellation_requested"] = ctx.cancellation_requested  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is the documented reading.
        observed["should_abort"] = ctx.should_abort()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; same documented reading.
        try:
            ctx.check_cancelled()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is the raise.
        except asyncio.CancelledError:
            observed["check_cancelled_raised"] = True
        return {"ok": True}

    backend.register_stub("probe", probe)
    args = EnqueueArgs(
        id=new_job_id(),
        actor="probe",
        queue="default",
        payload={},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    cancel_event = asyncio.Event()
    cancel_event.set()
    backend.register_cancel_event(args.id, cancel_event)
    await backend.enqueue(args)
    await backend.run_until_drained()

    row = await backend.get(args.id)
    assert row is not None
    assert row.status == "succeeded", (
        "the stub caught the CancelledError itself, so the job succeeds; "
        f"got status={row.status} error_class={row.error_class}"
    )
    assert observed == {
        "cancellation_requested": True,
        "should_abort": True,
        "check_cancelled_raised": True,
    }, f"the cancellation surface must observe the requested cancel; got {observed}"
