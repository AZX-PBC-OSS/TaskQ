"""The actor-facing surface of the in-memory runner's stub context.

``_StubContext`` is a declared minimal subset of the production
``taskq.context.JobContext`` — the members actors read under
``run_until_drained``. Two contracts are pinned here:

* ``span`` — the documented trace-correlation field
  (``docs/guides/observability.md``: "``ctx.span`` is the live consumer
  span, or ``None`` when OTel is disabled"). The in-memory runner is
  uninstrumented, so the parity-correct reading is ``None`` — exactly
  what a production worker without a tracer hands the actor — and the
  read itself must not fail. This is the runner half of issue #172;
  the mirror half is guarded by
  ``tests/test_job_context_mirror_field_parity.py``, which excludes
  ``_StubContext`` from its field walk (declared minimal subset), so
  the runner half is behaviour-pinned here instead.

* The documented method surface — ``await ctx.progress(...)``,
  ``ctx.check_cancelled()``, and ``ctx.should_abort()``
  (``docs/guides/progress.md`` teaches progress reporting as a headline
  actor feature, with a no-Redis PG fallback, so it is core, not
  optional; ``should_abort`` is the documented cooperative-cancellation
  check sync actors poll). Production carries all four actor-facing
  members (``cancellation_requested``, ``check_cancelled``,
  ``should_abort``, ``progress`` — ``src/taskq/context.py:82-103``);
  the runner's stub context has only ``cancellation_requested``, and
  the testing mirror has ``cancellation_requested`` and
  ``should_abort`` but neither ``progress`` nor ``check_cancelled``
  (tracked on issue #172's thread); the pin below is strict-xfail per
  the repo's executably-tracked-defect convention so the gap cannot sit
  silent in the suite while the narrower ``span`` field carries the
  convention's protection. Any actor that reports progress or polls
  cooperative cancellation is untestable through the harness today and
  fails with a self-misattributing ``AttributeError`` — the pin makes
  that failure the suite's own signal instead of the adopter's
  surprise.
"""

from datetime import UTC, datetime

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


async def test_stub_context_span_read_matches_the_documented_disabled_value() -> None:
    """An actor reading ``ctx.span`` through ``run_until_drained``
    observes ``None`` — the documented OTel-disabled value — and the job
    succeeds. A failing read would be the issue-#171/#172 drift class on
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


@pytest.mark.xfail(
    strict=True,
    reason="issue #172's method surface: the runner's stub context carries "
    "none of the documented actor-facing methods except "
    "cancellation_requested (progress, check_cancelled, should_abort), and "
    "the testing mirror carries neither progress nor check_cancelled — "
    "actors reporting progress or polling cooperative cancellation are "
    "untestable through the harness, failing with a self-misattributing "
    "AttributeError; fixed when the harness contexts carry the surface "
    "(progress observably recorded) or fail it with a designed, "
    "self-attributing unsupported-feature error — then remove this marker",
)
async def test_documented_method_surface_is_exercisable_through_the_runner() -> None:
    """An actor calling the documented ``await ctx.progress(...)``,
    ``ctx.check_cancelled()``, and ``ctx.should_abort()`` through
    ``run_until_drained`` gets either a working surface (the report
    lands observably; the job succeeds) or a designed, self-attributing
    unsupported-feature failure. It must never get the current shape —
    a bare ``AttributeError`` the actor misreads as its own bug. Both
    resolutions are legitimate; the absence is neither, and that is
    what this pin holds out."""
    clock = FakeClock(start=_START)
    backend = InMemoryBackend(clock=clock)
    gaps: list[str] = []

    async def reporter(payload: object, ctx: object) -> object:
        try:
            await ctx.progress(step=1, percent=50.0)  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the pinned contract is the disjunction recorded-or-designed-failure, never bare absence.
        except AttributeError:
            gaps.append("progress")
        try:
            ctx.check_cancelled()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; same disjunction contract as progress above.
        except AttributeError:
            gaps.append("check_cancelled")
        try:
            ctx.should_abort()  # type: ignore[attr-defined]  # Why: stub ctx is duck-typed; the sync-actor cooperative-cancellation check is part of the same documented surface.
        except AttributeError:
            gaps.append("should_abort")
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

    assert gaps == [], (
        "the documented method surface is absent on the harness context "
        f"(missing: {gaps}) — an actor reporting progress through the "
        "harness must be exercisable, not surprised by a bare "
        "AttributeError it will misattribute to its own code"
    )
