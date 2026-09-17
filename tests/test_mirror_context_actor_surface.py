"""The actor-facing surface of the testing JobContext mirror, exercised
through the ``actor_runner`` fixture.

Sibling of ``tests/test_stub_context_actor_surface.py``: that file pins
the runner's ``_StubContext`` (a declared minimal subset); this file pins
the full testing mirror (``taskq/testing/job_context.py``), whose own
docstring claims field-shape parity with production. Two contracts:

* The members the mirror DOES carry keep working through the harness —
  ``cancellation_requested`` and ``should_abort()`` (its partial
  cancellation surface) and ``span`` reading ``None`` (the documented
  OTel-disabled value, ``docs/guides/observability.md``). A regression
  that drops one of these is silent today without this control.

* The method surface — ``await ctx.progress(...)`` and
  ``ctx.check_cancelled()`` (``docs/guides/progress.md`` teaches
  progress reporting as a headline actor feature) — now carried by the
  mirror: progress reports land observably on the
  context's ``progress_reports`` with a strictly monotone ``seq`` (the
  fixture path has no Redis/Postgres wiring, so recording, not
  publishing, is the faithful harness half), and ``check_cancelled()``
  raises ``asyncio.CancelledError`` on a cancelled dispatch exactly as
  production does. The runner sibling
  (``tests/test_stub_context_actor_surface.py``) pins ``_StubContext``;
  this file pins the mirror, so neither half of the surface can
  regress while the other stays green.
"""

import asyncio

from pydantic import BaseModel

from taskq.testing.fixtures import ActorRunnerCallable
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.job_context import JobContext


async def test_mirror_context_present_surface_works_through_actor_runner(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """The surface the mirror carries today is observable through the
    fixture: ``cancellation_requested`` and ``should_abort()`` both read
    False on a fresh dispatch, and ``span`` reads None — the documented
    OTel-disabled value an uninstrumented production worker hands the
    actor. Pins the present surface so a regression dropping any of
    these goes red here, not in an adopter's actor test."""
    observed: dict[str, object] = {}

    def reader(payload: object, ctx: JobContext[BaseModel]) -> object:
        observed["cancellation_requested"] = ctx.cancellation_requested
        observed["should_abort"] = ctx.should_abort()
        observed["span"] = ctx.span
        return {"ok": True}

    result = await actor_runner(reader, {}, backend=memory_jobs)

    assert result == {"ok": True}
    assert observed == {
        "cancellation_requested": False,
        "should_abort": False,
        "span": None,
    }, f"the mirror's present surface regressed: {observed}"


async def test_documented_method_surface_is_exercisable_through_the_mirror(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """An actor calling the documented ``await ctx.progress(...)`` and
    ``ctx.check_cancelled()`` through ``actor_runner`` succeeds, and the
    progress report lands observably on the context — recorded with a
    strictly monotone ``seq``, the faithful harness half of a contract
    whose production half publishes. The runner sibling
    (``tests/test_stub_context_actor_surface.py``) holds the same
    contract for ``_StubContext``; both harness surfaces carry the
    surface, and each pin holds its own."""
    contexts: list[JobContext[BaseModel]] = []

    async def reporter(payload: object, ctx: JobContext[BaseModel]) -> object:
        contexts.append(ctx)
        await ctx.progress(step=1, percent=50.0)
        await ctx.progress(step=2)
        ctx.check_cancelled()
        return {"ok": True}

    result = await actor_runner(reporter, {}, backend=memory_jobs)

    assert result == {"ok": True}
    assert contexts[0].progress_reports == [
        {"seq": 1, "step": 1, "percent": 50.0, "detail": None, "data": None},
        {"seq": 2, "step": 2, "percent": None, "detail": None, "data": None},
    ], (
        "each report must land observably with a strictly monotone seq; "
        f"got {contexts[0].progress_reports}"
    )


async def test_mirror_context_check_cancelled_raises_on_a_cancelled_dispatch(
    actor_runner: ActorRunnerCallable,
    memory_jobs: InMemoryBackend,
) -> None:
    """The raising half of the cancellation contract: with a pre-fired
    cancel event, ``check_cancelled()`` raises
    :class:`asyncio.CancelledError` — matching production. The mirror
    keeps production's two cancellation primitives distinct:
    ``should_abort()`` reads ``abort_requested`` (set by the cancel
    controller, not by the event), so it stays False here exactly as a
    production context whose controller has not fired would read."""
    cancel_event = asyncio.Event()
    cancel_event.set()
    observed: dict[str, object] = {}

    def probe(payload: object, ctx: JobContext[BaseModel]) -> object:
        observed["cancellation_requested"] = ctx.cancellation_requested
        observed["should_abort"] = ctx.should_abort()
        try:
            ctx.check_cancelled()
        except asyncio.CancelledError:
            observed["check_cancelled_raised"] = True
        return {"ok": True}

    result = await actor_runner(probe, {}, backend=memory_jobs, cancel_event=cancel_event)

    assert result == {"ok": True}
    assert observed == {
        "cancellation_requested": True,
        "should_abort": False,
        "check_cancelled_raised": True,
    }, f"the mirror's cancellation surface must observe the fired event; got {observed}"
