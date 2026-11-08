"""The holds-SSE consumer's resource controls.

Every ``_holds_generator`` exit after listener construction must close the
frames iterator AND stop the listener — including when the consumer's own
cancellation is re-delivered while the bounded teardown is in flight. Each
``C`` identifier below is one behavior scenario; most scenarios are one
collected test node, and the parametrized scenarios (C8, C13, C16, and the
C18/C19 pair) report each expansion as its own collected node.

The listener-bearing scenarios pin this resource contract: the frames
iterator unsubscribes exactly once (when iteration started), the listener
stop runs exactly once, the captured ``taskq-hitl-listener`` pump task
terminates, the LISTEN connection is returned exactly once through the real
shielded release path, the route's semaphore is restored, and the connection
metric closes exactly once. The qualified arms differ truthfully: C8's
attached timeout arm expects the close cancelled by its own bound before
its unsubscribe (unsubscribes 0), the C13 initial arm, C14, and C16 never
fabricate a REAL listener, pump, or connection (the C14 start failure is a
failing call on the fake listener; no real database resource exists; their
semaphore/metric still close exactly once), and C16's degraded keepalive
stream never reaches the listener path at all. The listener's stop detaches
the pump slot before reaping, so the
slot's state is the direct stop observation; the settled-state helpers below
assert the full contract through one ordered block per scenario.

The doubles replace only the listener/session boundary the generator uses
and mirror the real machinery's shape: the fake pump is a real
``asyncio.Task`` with the production name whose cleanup rides the real
``release_listen_connection`` shield; every gated ``stop()`` cancels its
pump synchronously before its first await (``reap_cancelled_child`` is
stop's first await, exactly as ``_BroadcastListener.stop`` does) and only
the subsequent reap/pump-release phase is gated — a gate anywhere before
that synchronous cancel would be a shape the real listener never produces.
The fakes never swallow exceptions, never report a cleanup that did not
run, and every resource pin asserts an exact count (a skipped teardown
operation is a failing assertion, not a permissive default).
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import cast

import asyncpg
import pytest
import structlog.testing

pytest.importorskip("fastapi")

from asyncpg.pool import PoolConnectionProxy
from starlette.responses import StreamingResponse

import taskq.web.admin.sse as sse_mod
import taskq.workflows.api._hitl_listen as hitl_mod
import taskq.workflows.api._listen_pump as listen_pump_mod
from taskq._reaper import reap_cancelled_child
from taskq.obs import SseSurface, record_sse_connection_closed
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin.sse import _holds_generator
from taskq.workflows.api._hitl_listen import HitlListener, HoldEvent
from taskq.workflows.api._listen_pump import release_listen_connection

pytestmark = [pytest.mark.fastapi]

#: The production pump task's name.
_PUMP_TASK_NAME = "taskq-hitl-listener"

#: The fake listener never touches the pool — the generator forwards it to
#: the (replaced) ``HitlListener`` constructor, whose patched factory discards
#: every argument, so no pool operation is reachable from the marker. The
#: cast only satisfies the generator's pool parameter type at the one factory
#: boundary; the marker carries no behavior.
_UNTOUCHED_POOL: asyncpg.Pool = cast("asyncpg.Pool", object())


def _untouched_pool() -> asyncpg.Pool | None:
    return _UNTOUCHED_POOL


def _permits(sem: asyncio.Semaphore) -> int:
    return sem._value  # pyright: ignore[reportPrivateUsage]  # Why: no public API reads a semaphore's permit count; the exactly-once release pin needs it.


def _pending_others() -> list[str]:
    me = asyncio.current_task()
    return sorted(t.get_name() for t in asyncio.all_tasks() if t is not me and not t.done())


async def _bounded(condition: Callable[[], bool], deadline_secs: float = 3.0) -> None:
    """Bounded poll for a condition the loop's own scheduling must settle
    (a detached task's completion, a callback's side effect). Exposes the
    pending tasks in the failure instead of sleeping for a guessed grace
    period."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    while not condition():
        if loop.time() - start > deadline_secs:
            raise TimeoutError(
                f"bounded wait expired after {deadline_secs}s; pending tasks: {_pending_others()}"
            )
        # Bounded poll for state only the loop's scheduling can settle; no
        # Event exists to wait on.
        await asyncio.sleep(0)


async def _drained(deadline_secs: float = 3.0) -> bool:
    """Best-effort bounded drain for containment paths (the exact quiescence
    assertions live in the settled-state helpers)."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    while _pending_others():
        if loop.time() - start > deadline_secs:
            return False
        # Bounded containment drain; the suite's leak guard names anything
        # that survives.
        await asyncio.sleep(0)
    return True


def _print_diagnostics(label: str, facts: dict[str, object]) -> None:
    """Print the scenario's structured resource record before the pump-slot
    observation, so any failure carries the full resource facts in the test
    output."""
    print(f"sse-holds-{label}-diagnostics {json.dumps(facts, sort_keys=True, default=repr)}")


def _spy_metric_closes(monkeypatch: pytest.MonkeyPatch) -> list[tuple[SseSurface, str]]:
    """Delegating spy on the sse module's metric-close call site: records each
    (surface, topic) close and delegates to the real recorder untouched."""
    closes: list[tuple[SseSurface, str]] = []
    real_closed = record_sse_connection_closed

    def _counting_closed(surface: SseSurface, topic: str) -> None:
        closes.append((surface, topic))
        real_closed(surface, topic)

    monkeypatch.setattr(sse_mod, "record_sse_connection_closed", _counting_closed)
    return closes


class _LoopExceptions:
    """Scoped loop exception-handler capture: installs a capturing handler,
    records every full handler context (never just booleans), and restores
    the exact prior handler. The context type is the capture boundary's own
    ``dict[str, object]``: every handler key this suite reads (``message``,
    ``exception``) is read through exact key/identity checks, and the
    capture never suppresses or rewrites anything."""

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._prior = self._loop.get_exception_handler()
        self.contexts: list[dict[str, object]] = []

    def install(self) -> None:
        capture = self

        def _capture_handler(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
            capture.contexts.append(context)

        self._loop.set_exception_handler(_capture_handler)

    def restore(self) -> None:
        self._loop.set_exception_handler(self._prior)

    def shield_reports(self, message: str) -> list[dict[str, object]]:
        """Every captured context whose message is exactly *message* (the
        stdlib ``asyncio.shield`` detached-failure report's identity)."""
        return [ctx for ctx in self.contexts if ctx.get("message") == message]

    def never_retrieved(self) -> list[dict[str, object]]:
        """Every captured context whose message reports an unretrieved task
        exception — none is ever permitted for these controls."""
        return [
            ctx
            for ctx in self.contexts
            if "Task exception was never retrieved" in str(ctx.get("message", ""))
        ]


class FatalError(BaseException):
    """A custom non-``Exception`` ``BaseException`` — the exact propagated
    identity for the controls that inject a ``BaseException`` (an ``except
    Exception`` policy must never catch it)."""


class _FakeConn:
    """Connection stand-in for the real ``release_listen_connection`` cleanup:
    the UNLISTEN steps are immediate no-ops; the release is the pool's."""

    async def remove_listener(self, channel: str, cb: object) -> None:
        return None

    async def execute(self, sql: str) -> None:
        return None


class _GatedReleasePool:
    """Counting acquire/release site for the real ``release_listen_connection``:
    the release phase can be gated (parked mid-cleanup) so a test can cancel
    the consumer while the pump is inside its own release, and every release
    records the exact connection only after it ran — a release that never ran
    is never reported as run."""

    def __init__(self) -> None:
        self.conn = _FakeConn()
        self.acquired: list[_FakeConn] = []
        self.releases: list[_FakeConn] = []
        self.release_started = asyncio.Event()
        self.release_gate = asyncio.Event()
        self.release_gate.set()
        self.released = asyncio.Event()

    async def acquire(self) -> _FakeConn:
        self.acquired.append(self.conn)
        return self.conn

    async def release(self, conn: _FakeConn) -> None:
        self.release_started.set()
        await self.release_gate.wait()
        self.releases.append(conn)
        self.released.set()


class _FakeFrames:
    """``HitlListener.frames`` double for the shapes the controls exercise:
    keepalive sentinels, injected failures, ordinary exhaustion, and an
    ``aclose`` whose start is observable and whose body is gatable. The
    unsubscribe is counted only when iteration actually started (the real
    ``_iterate`` subscribes on its first ``__anext__`` and unsubscribes from
    its own finally — nothing is unsubscribed if the iterator never ran)."""

    def __init__(self) -> None:
        self.items: asyncio.Queue[BaseException | HoldEvent | None] = asyncio.Queue()
        self.items.put_nowait(None)  # the first tick: a keepalive
        self.iteration_started = False
        self.close_started = asyncio.Event()
        self.close_gate = asyncio.Event()
        self.close_gate.set()
        self.unsubscribes = 0
        self.close_error: BaseException | None = None
        self.exhausted = False

    def __aiter__(self) -> "_FakeFrames":
        return self

    async def __anext__(self) -> HoldEvent | None:
        self.iteration_started = True
        if self.exhausted:
            raise StopAsyncIteration
        item = await self.items.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def aclose(self) -> None:
        self.close_started.set()
        await self.close_gate.wait()
        if self.iteration_started:
            self.unsubscribes += 1
        if self.close_error is not None:
            raise self.close_error


class _FakeListener:
    """Duck-typed ``HitlListener`` at the exact boundary the generator uses
    (``start`` / ``frames`` / ``stop``), with the real ``_BroadcastListener``
    stop shape: the pump slot is detached and the pump cancelled
    SYNCHRONOUSLY before stop's first await, then reaped through the real
    ``reap_cancelled_child``; the pump's own cleanup rides the real
    ``release_listen_connection`` shield with the release site gated. The
    injected ``stop_error`` is raised only after that reap phase — an
    explicitly raised stop exception, never a manufactured timeout."""

    def __init__(self) -> None:
        self.frames_obj = _FakeFrames()
        self.pool = _GatedReleasePool()
        self.stop_calls = 0
        self.start_calls = 0
        self.start_error: BaseException | None = None
        self.stop_error: BaseException | None = None
        self.cancel_before_first_await: list[bool] = []
        self._pump: asyncio.Task[None] | None = None

    def frames(self) -> _FakeFrames:
        return self.frames_obj

    async def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error
        if self._pump is None:
            self._pump = asyncio.create_task(self._pump_body(), name=_PUMP_TASK_NAME)

    async def _pump_body(self) -> None:
        conn = await self.pool.acquire()
        try:
            await asyncio.sleep(3600)
        finally:
            # The real pump's own release path: the cleanup is shielded and
            # its outcome retrieved (release_listen_connection), so a second
            # cancellation cascading through the reap chain cannot strand the
            # release — the shape the real listener converges with.
            await release_listen_connection(
                conn,  # pyright: ignore[reportArgumentType]  # Why: test duck-type conn; identity and the gated release are all the fake needs.
                self.pool,  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool; the counting release site is the observation point.
                ("holds",),
                lambda *_args: None,
            )

    async def stop(self) -> None:
        self.stop_calls += 1
        task, self._pump = self._pump, None
        if task is not None:
            task.cancel()  # synchronous, before stop's first await — the real order
            self.cancel_before_first_await.append(task.cancelling() >= 1)
            await reap_cancelled_child(task)
        if self.stop_error is not None:
            raise self.stop_error

    async def test_only_cleanup(self) -> None:
        """Containment only, after every assertion: open every gate, clear
        every injected error, stop and reap the pump if it ever existed."""
        self.frames_obj.close_gate.set()
        self.frames_obj.close_error = None
        self.pool.release_gate.set()
        self.stop_error = None
        if self._pump is not None:
            await self.stop()


def _pump_settled(listener: _FakeListener, pump: asyncio.Task[None] | None) -> bool:
    """The settled fake-pump shape: the slot detached (stop entered) and the
    captured pump task terminal."""
    return (
        listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        and pump is not None
        and pump.done()
    )


async def _assert_fake_settled(
    listener: _FakeListener,
    harness: "_HoldsHarness",
    pump: asyncio.Task[None] | None,
    *,
    stop_calls: int = 1,
    unsubscribes: int = 1,
) -> None:
    """The fully settled fake-listener contract, in one ordered block: the
    detached teardown has finished (pump terminal, release recorded), the
    stop ran exactly *stop_calls* times (the settled second stop control
    asserts its own count locally), the unsubscribe ran exactly
    *unsubscribes* times, the acquired connection was released exactly once,
    the semaphore/metric closed exactly once, and no scenario-created task is
    left pending."""
    await _bounded(lambda: _pump_settled(listener, pump) and listener.pool.released.is_set())
    assert listener.stop_calls == stop_calls
    assert pump is not None and pump.done()
    assert listener.cancel_before_first_await == [True]
    assert listener.frames_obj.unsubscribes == unsubscribes
    assert listener.pool.acquired == [listener.pool.conn]
    assert listener.pool.releases == [listener.pool.conn]
    assert _permits(harness.sem) == 1
    assert len(harness.metric_closes) == 1
    await _bounded(lambda: not _pending_others())


def _fake_resource_facts(
    listener: _FakeListener,
    harness: "_HoldsHarness",
    pump: asyncio.Task[None] | None,
) -> dict[str, object]:
    """The common resource facts for the fake-listener scenarios' diagnostics;
    each scenario merges its own unique fields before printing."""
    return {
        "stop_calls": listener.stop_calls,
        "pump_pending": pump is not None and not pump.done(),
        "pump_is_original_captured": listener._pump is pump,  # pyright: ignore[reportPrivateUsage]  # Why: as above.
        "unsubscribes": listener.frames_obj.unsubscribes,
        "acquires": len(listener.pool.acquired),
        "releases": len(listener.pool.releases),
        "semaphore_permits": _permits(harness.sem),
        "metric_closes": len(harness.metric_closes),
        "pending_tasks": _pending_others(),
    }


class _HoldsHarness:
    """Replaces ``hitl_mod.HitlListener`` with one fake listener instance for
    the test's duration (the generator's lazy import reads the module
    attribute at every generator start) and spies the metric-close site."""

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        verifier: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        self.listener = _FakeListener()
        self.sem = asyncio.Semaphore(1)
        self.verifier = verifier
        self.metric_closes = _spy_metric_closes(monkeypatch)
        self.gen: AsyncGenerator[str, None] | None = None
        harness = self

        def factory(*args: object, **kwargs: object) -> _FakeListener:
            return harness.listener

        monkeypatch.setattr(hitl_mod, "HitlListener", factory)

    async def prime(self) -> AsyncGenerator[str, None]:
        """The route's pre-acquired semaphore, then the generator driven to
        its first yielded frame (the keepalive) — the listener is started and
        its pump is live."""
        await self.sem.acquire()
        gen = _holds_generator(self.sem, _untouched_pool, "taskq", self.verifier)
        frame = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        assert frame == ": keepalive\n\n", f"first frame was {frame!r}, not the keepalive"
        self.gen = gen
        return gen

    async def cleanup(self) -> None:
        if self.gen is not None:
            await self.gen.aclose()
        await self.listener.test_only_cleanup()
        await _drained()


class _RevokingVerifier:
    """Session verifier passing the first *passes* checks, then revoking —
    the C13 session-rejection placements."""

    def __init__(self, passes: int) -> None:
        self._passes = passes
        self.calls = 0

    async def __call__(self) -> bool:
        self.calls += 1
        return self.calls <= self._passes


class _RealFramesGate:
    """Wraps the REAL ``listener.frames()`` generator: ``aclose`` starts,
    holds on a gate, then delegates — the deterministic placement of the
    mid-close cancellation for the real-listener controls. Iteration
    delegates untouched."""

    def __init__(self, inner: AsyncGenerator[HoldEvent | None, None]) -> None:
        self._inner = inner
        self.close_started = asyncio.Event()
        self.close_gate = asyncio.Event()
        self.close_gate.set()

    def __aiter__(self) -> "_RealFramesGate":
        return self

    async def __anext__(self) -> HoldEvent | None:
        return await self._inner.__anext__()

    async def aclose(self) -> None:
        self.close_started.set()
        await self.close_gate.wait()
        await self._inner.aclose()


class _ReleaseSpyPool:
    """Typed delegating spy around the REAL asyncpg pool face the pump uses.
    Only ``acquire`` and ``release`` exist — any other pool operation on the
    spy is a loud ``AttributeError``, never a permissive default. Both
    methods delegate to the real asyncpg pool and preserve its identity,
    result, and failures untouched. The release phase can be gated so a test
    can hold the real pump mid-release; the spy records three separate
    facts — the acquired connection identity, the release's start, and the
    release's COMPLETION (recorded only after the real release returned
    successfully), so an attempted-but-failed release is never counted as
    returned."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self.acquired: list[PoolConnectionProxy[asyncpg.Record]] = []
        self.release_started = asyncio.Event()
        self.release_gate = asyncio.Event()
        self.release_gate.set()
        self.released_completed: list[PoolConnectionProxy[asyncpg.Record]] = []
        self.release_completed = asyncio.Event()

    async def acquire(self) -> PoolConnectionProxy[asyncpg.Record]:
        conn = await self._pool.acquire()
        self.acquired.append(conn)
        return conn

    async def release(self, conn: PoolConnectionProxy[asyncpg.Record]) -> None:
        self.release_started.set()
        await self.release_gate.wait()
        await self._pool.release(conn)
        self.released_completed.append(conn)
        self.release_completed.set()


def _spied_pool(spy: _ReleaseSpyPool) -> Callable[[], asyncpg.Pool | None]:
    """The generator's ``resolve_pool`` handing the test's spy to the
    listener. The cast is the one nominal-type boundary: ``asyncpg.Pool`` is
    the constructor's nominal annotation, and the spy is deliberately NOT a
    general pool — it implements exactly the two async faces the listener's
    pump reaches (``acquire``/``release``), both delegating to the real pool
    it wraps with identity/result/failure preserved, and any other pool
    operation raises loudly. The real-PG controls assert the concrete
    acquire/release operations through it, so the cast cannot conceal a
    no-op."""

    def _resolve() -> asyncpg.Pool | None:
        return cast("asyncpg.Pool", spy)

    return _resolve


class _RealListenerCapture:
    """Installs a factory for the REAL ``HitlListener`` (the module attribute
    the generator's lazy import reads) that records every created listener
    and, when *gated*, wraps its ``frames()`` in the gated iterator."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, gated: bool) -> None:
        self.listeners: list[HitlListener] = []
        self.gates: list[_RealFramesGate] = []
        real_cls = hitl_mod.HitlListener
        capture = self

        def factory(
            pool: asyncpg.Pool, schema: str, *, keepalive_interval: float = 30.0
        ) -> HitlListener:
            listener = real_cls(pool, schema=schema, keepalive_interval=keepalive_interval)
            capture.listeners.append(listener)
            if gated:
                wrapper = _RealFramesGate(listener.frames())
                capture.gates.append(wrapper)
                monkeypatch.setattr(listener, "frames", lambda: wrapper)
            return listener

        monkeypatch.setattr(hitl_mod, "HitlListener", factory)


def _spy_real_stop(monkeypatch: pytest.MonkeyPatch, listener: HitlListener) -> list[HitlListener]:
    """Delegating entry recorder for the REAL ``listener.stop()``: proves
    whether the real stop was ever entered without altering its behavior."""
    calls: list[HitlListener] = []
    real_stop = listener.stop

    async def _counting_stop() -> None:
        calls.append(listener)
        await real_stop()

    monkeypatch.setattr(listener, "stop", _counting_stop)
    return calls


def _real_pump(listener: HitlListener) -> asyncio.Task[None] | None:
    return listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.


def _real_subscriber_count(listener: HitlListener) -> int:
    return len(listener._fanout._subs)  # pyright: ignore[reportPrivateUsage]  # Why: the settled-empty frame registry is the unsubscribe observable; no public reader.


def _real_pump_settled(listener: HitlListener, pump: asyncio.Task[None] | None) -> bool:
    return _real_pump(listener) is None and pump is not None and pump.done()


async def _assert_real_settled(
    listener: HitlListener,
    pump: asyncio.Task[None] | None,
    spy: _ReleaseSpyPool,
    pool: asyncpg.Pool,
    sem: asyncio.Semaphore,
    metric_closes: list[tuple[SseSurface, str]],
    stop_entries: list[HitlListener],
    conn: PoolConnectionProxy[asyncpg.Record],
) -> None:
    """The fully settled real-listener contract, in one ordered block: the
    detached teardown has finished (pump slot detached, captured pump
    terminal), the real stop ran exactly once, the frame registry is empty,
    the exact acquired LISTEN connection completed its release exactly once,
    the real pool balances, the semaphore/metric closed exactly once, and no
    scenario-created task is left pending."""
    await _bounded(lambda: _real_pump_settled(listener, pump) and len(spy.released_completed) == 1)
    assert len(stop_entries) == 1
    assert _real_pump(listener) is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
    assert pump is not None and pump.done()
    assert _real_subscriber_count(listener) == 0
    assert spy.released_completed == [conn]
    assert pool.get_size() == pool.get_idle_size()
    assert _permits(sem) == 1
    assert len(metric_closes) == 1
    await _bounded(lambda: not _pending_others())


def _real_resource_facts(
    listener: HitlListener,
    pump: asyncio.Task[None] | None,
    spy: _ReleaseSpyPool,
    pool: asyncpg.Pool,
    sem: asyncio.Semaphore,
    metric_closes: list[tuple[SseSurface, str]],
    stop_entries: list[HitlListener],
    conn: PoolConnectionProxy[asyncpg.Record],
) -> dict[str, object]:
    """The common resource facts for the real-listener scenarios' diagnostics;
    each scenario merges its own unique fields before printing."""
    return {
        "real_stop_entered": len(stop_entries),
        "pump_pending": pump is not None and not pump.done(),
        "pump_task_name": pump.get_name() if pump is not None else None,
        "release_completions_for_acquired_connection": sum(
            1 for completed in spy.released_completed if completed is conn
        ),
        "pool_size": pool.get_size(),
        "pool_idle": pool.get_idle_size(),
        "subscriber_count": _real_subscriber_count(listener),
        "semaphore_permits": _permits(sem),
        "metric_closes": len(metric_closes),
        "pending_tasks": _pending_others(),
    }


# ── C1-C2: ordinary exits settle everything ──────────────────────────────


async def test_c1_ordinary_aclose_at_yield_settles_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C1 — an ordinary ``gen.aclose()`` at a yielded frame (the
    client-disconnect shape) closes the frames exactly once, stops the
    listener exactly once, terminates the captured pump, releases the LISTEN
    connection exactly once through the real shielded release path, and
    closes the semaphore/metric exactly once; a settled second close is
    inert."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        await gen.aclose()
        await gen.aclose()  # the settled second close: inert
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


async def test_c2_single_body_cancel_settles_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C2 — ONE explicit ``Task.cancel()`` during the body's wait raises
    ``CancelledError`` through the consumer and the teardown still settles
    every resource exactly once."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        stream = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0)
        stream.cancel("one body cancellation")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stream, timeout=2.0)
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


# ── C3/C4/C7/C10-MID-CLOSE/C11b: re-delivered cancellation and failure windows ──


async def test_c3_second_cancel_during_frame_close_stops_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C3 — a second explicit ``Task.cancel()`` delivered while the gated
    ``frames.aclose()`` holds the teardown must still end in
    ``listener.stop()``: the consumer observes its cancellation, the frames
    unsubscribe exactly once, and the captured pump terminates with its
    connection released exactly once. Stop detaches the pump slot before
    reaping, so the slot's state is the direct stop observation, taken
    before the later release/settlement assertions."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    listener.frames_obj.close_gate.clear()  # hold frames.aclose() mid-flight
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        stream = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0)
        stream.cancel("initial body cancellation")
        await asyncio.wait_for(listener.frames_obj.close_started.wait(), timeout=2.0)
        stream.cancel("re-delivered cancellation at the gated frames close")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stream, timeout=2.0)
        listener.frames_obj.close_gate.set()
        await _bounded(lambda: listener.frames_obj.unsubscribes == 1)
        facts = _fake_resource_facts(listener, harness, pump)
        facts["consumer_cancelled"] = True
        _print_diagnostics("c3", facts)
        # Stop detaches the pump slot before reaping, so a still-attached
        # slot here is the skipped-stop state itself; the release and
        # settlement assertions below are ordered after it.
        assert listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


async def test_c4_asgi20_disconnect_during_frame_close_stops_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C4 — an actual Starlette ASGI ``spec_version`` 2.0 ``http.disconnect``
    cancels the response's anyio cancel scope, which re-delivers cancellation
    at every later checkpoint with NO second explicit ``asyncio`` cancel;
    while the gated ``frames.aclose()`` holds the teardown,
    ``listener.stop()`` must still run and the captured pump must terminate
    with its connection released. The legacy Starlette branch is pinned
    explicitly here — never presented as every server's implementation."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    listener.frames_obj.close_gate.clear()  # hold frames.aclose() mid-flight
    await harness.sem.acquire()  # the route's pre-acquire
    gen = _holds_generator(harness.sem, _untouched_pool, "taskq")
    harness.gen = gen
    messages: list[dict[str, object]] = []
    inbound: asyncio.Queue[dict[str, str]] = asyncio.Queue()
    first_body = asyncio.Event()

    async def receive() -> dict[str, str]:
        return await inbound.get()

    async def send(message: dict[str, object]) -> None:
        messages.append(message)
        if message["type"] == "http.response.body" and message.get("body"):
            first_body.set()

    try:
        response = StreamingResponse(gen)
        scope: dict[str, object] = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "method": "GET",
            "path": "/sse/holds",
            "headers": [],
            "query_string": b"",
        }
        call = asyncio.create_task(response(scope, receive, send))
        await asyncio.wait_for(first_body.wait(), timeout=2.0)
        await inbound.put({"type": "http.disconnect"})
        await asyncio.wait_for(call, timeout=2.0)
        await asyncio.wait_for(listener.frames_obj.close_started.wait(), timeout=2.0)
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        listener.frames_obj.close_gate.set()
        await _bounded(lambda: listener.frames_obj.unsubscribes == 1)
        facts = _fake_resource_facts(listener, harness, pump)
        facts.update(
            {
                "http_disconnect_delivered": True,
                "response_returned": call.done() and call.exception() is None,
                "response_exception": repr(call.exception()) if call.done() else None,
                "explicit_cancels_after_disconnect": 0,
                "sent_message_types": [m.get("type") for m in messages],
            }
        )
        _print_diagnostics("c4", facts)
        # The pump-slot observation precedes the release/settlement tail.
        assert listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


async def test_c7_body_failure_then_repeat_cancel_at_first_frame_close_stops_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C7 — an active body/iterator failure (the frames iterator raises
    ``RuntimeError`` into the consumer's async-for) enters the teardown, and
    a repeat cancel lands while the FIRST gated frame close is still held.
    Cancellation observed at the teardown shield wins the consumer's
    outcome; ``listener.stop()`` must still run and the captured pump must
    terminate with its connection released."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    listener.frames_obj.close_gate.clear()  # hold the FIRST frame close mid-flight
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        body_error = RuntimeError("body failed: c7")
        listener.frames_obj.items.put_nowait(body_error)
        stream = asyncio.create_task(gen.__anext__())
        await asyncio.wait_for(listener.frames_obj.close_started.wait(), timeout=2.0)
        stream.cancel("repeat cancel during the first gated frame close")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stream, timeout=2.0)
        listener.frames_obj.close_gate.set()
        await _bounded(lambda: listener.frames_obj.unsubscribes == 1)
        facts = _fake_resource_facts(listener, harness, pump)
        facts["consumer_cancelled"] = True
        facts["body_error_before_cancellation"] = repr(body_error)
        _print_diagnostics("c7", facts)
        # The pump-slot observation precedes the release/settlement tail.
        assert listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


async def test_c10_mid_close_repeat_cancel_at_combined_shield_detached_stop_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C10-MID-CLOSE — the consumer's repeat cancellation is observed at the
    combined teardown shield while ``frames.aclose()`` is gated; the
    detached teardown later reaches ``listener.stop()``, which fails with a
    unique injected ``RuntimeError`` whose identity the retrieval log and
    the scoped loop handler must carry. The pump slot stays attached until
    stop detaches it, so the slot observation comes before the retrieval-
    log/handler assertions, which run after the detached stop failure has
    actually settled."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    listener.frames_obj.close_gate.clear()  # hold frames.aclose() mid-flight
    detached_error = RuntimeError("detached stop failed: c10-unique-detached-token")
    listener.stop_error = detached_error
    loop_exceptions = _LoopExceptions()
    try:
        loop_exceptions.install()
        with structlog.testing.capture_logs() as logs:
            gen = await harness.prime()
            pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
            assert pump is not None and not pump.done()
            stream = asyncio.create_task(gen.__anext__())
            await asyncio.sleep(0)
            stream.cancel("initial body cancellation")
            await asyncio.wait_for(listener.frames_obj.close_started.wait(), timeout=2.0)
            stream.cancel("repeat cancel observed at the combined shield mid-close")
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(stream, timeout=2.0)
            listener.frames_obj.close_gate.set()
            await _bounded(lambda: listener.frames_obj.unsubscribes == 1)
            facts = _fake_resource_facts(listener, harness, pump)
            facts["consumer_cancelled"] = True
            facts["intended_detached_error"] = repr(detached_error)
            facts["captured_log_events"] = [log.get("event") for log in logs]
            facts["handler_context_messages"] = [
                str(ctx.get("message")) for ctx in loop_exceptions.contexts
            ]
            _print_diagnostics("c10_mid_close", facts)
            # The pump-slot observation precedes the retrieval-log/handler
            # assertions and the release/settlement tail.
            assert listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
            # The detached teardown settles, its stop fails, and the failure
            # is retrieved with exact identity.
            await _bounded(
                lambda: any(log.get("event") == "shield-detached-task-failed" for log in logs)
            )
            await _bounded(lambda: not _pending_others())
            taskq_entries = [
                log for log in logs if log.get("event") == "shield-detached-task-failed"
            ]
            assert len(taskq_entries) == 1
            assert taskq_entries[0].get("error_type") == "RuntimeError"
            assert taskq_entries[0].get("error") == repr(detached_error)
            shield_reports = loop_exceptions.shield_reports(
                "RuntimeError exception in shielded future"
            )
            assert len(shield_reports) == 1
            assert shield_reports[0].get("exception") is detached_error
            assert not loop_exceptions.never_retrieved()
            await _assert_fake_settled(listener, harness, pump)
    finally:
        await _drained()
        loop_exceptions.restore()
        await harness.cleanup()


async def test_c11b_frame_close_base_exception_still_stops_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C11b — a custom non-``Exception`` ``BaseException`` raised by
    ``frames.aclose()`` itself must still leave the teardown running
    ``listener.stop()`` (the stop belongs in the frame close's inner
    finally), and the EXACT injected ``BaseException`` object propagates to
    the consumer when stop succeeds (same-object identity, class, and
    message all asserted)."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    fatal = FatalError("fatal frame close: c11b-unique-token")
    listener.frames_obj.close_error = fatal
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        captured_fatal: FatalError | None = None
        try:
            await asyncio.wait_for(gen.aclose(), timeout=2.0)
        except FatalError as exc:
            captured_fatal = exc
        assert captured_fatal is fatal, (
            f"the exact injected frame-close BaseException object must propagate: got {captured_fatal!r}"
        )
        assert type(captured_fatal) is FatalError
        assert str(captured_fatal) == str(fatal)
        facts = _fake_resource_facts(listener, harness, pump)
        facts["propagated_frame_close_exception"] = (
            f"{type(captured_fatal).__name__}: {captured_fatal}"
        )
        facts["propagated_is_same_object"] = captured_fatal is fatal
        facts["propagated_class_matches"] = type(captured_fatal) is FatalError
        facts["propagated_message_matches"] = str(captured_fatal) == str(fatal)
        _print_diagnostics("c11b", facts)
        # The pump-slot observation precedes the release/settlement tail.
        assert listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


# ── C5/C6/C8/C9/C11a/C12/C13/C14/C15/C16: cancellation windows, failures, ──
#    idempotence, and degraded shapes


async def test_c5_cancel_during_gated_pump_release_after_synchronous_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C5 — the pump's ``.cancel()`` occurs first (synchronously, before
    stop's first await — recorded inside the double between the cancel and
    the reap) and the consumer's re-delivered cancellation lands during the
    gated reap/pump-release phase; the cancellation still propagates while
    stop, the pump, and the shielded release all finish after the gate
    opens, exactly once."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    listener.pool.release_gate.clear()  # hold the pump's release phase
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        stream = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0)
        stream.cancel("initial body cancellation")
        await asyncio.wait_for(listener.pool.release_started.wait(), timeout=2.0)
        cancel_count_at_release_start = pump.cancelling()
        assert not pump.done()  # the pump is mid-cleanup, gated
        stream.cancel("re-delivered cancellation during the gated pump release")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stream, timeout=2.0)
        listener.pool.release_gate.set()
        await _assert_fake_settled(listener, harness, pump)
        assert listener.cancel_before_first_await == [True]
        assert cancel_count_at_release_start >= 1
    finally:
        await harness.cleanup()


async def test_c6_iterator_failure_still_stops_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C6 — the frames iterator's ``RuntimeError`` is the consumer's outcome
    unchanged, and the teardown still unsubscribes, stops the listener,
    terminates the pump, and releases exactly once."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        listener.frames_obj.items.put_nowait(RuntimeError("iterator failed: c6"))
        with pytest.raises(RuntimeError, match="iterator failed: c6"):
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


@pytest.mark.parametrize("mode", ["runtime_error", "timeout"], ids=["runtime_error", "timeout"])
async def test_c8_attached_frame_close_failure_logged_and_stop_still_runs(
    monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """C8 — an ordinary ``Exception`` from the bounded frame close — a
    ``RuntimeError``, or the ``TimeoutError`` its own bound raises — is
    logged exactly once by ``admin-sse-holds-close-failed`` and the teardown
    still stops the listener and settles every resource. No loop context
    ever reports the frame-close failure as never retrieved."""
    loop_exceptions = _LoopExceptions()
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    expected_error_type = "RuntimeError"
    if mode == "runtime_error":
        listener.frames_obj.close_error = RuntimeError("frame close failed: c8")
    else:
        expected_error_type = "TimeoutError"
        monkeypatch.setattr(sse_mod, "CLOSE_TIMEOUT_SECS", 0.1)
        listener.frames_obj.close_gate.clear()  # the close wedges past its bound
    try:
        loop_exceptions.install()
        with structlog.testing.capture_logs() as logs:
            gen = await harness.prime()
            pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
            assert pump is not None and not pump.done()
            await asyncio.wait_for(gen.aclose(), timeout=5.0)  # logged-and-continued
            close_failures = [
                log for log in logs if log.get("event") == "admin-sse-holds-close-failed"
            ]
            assert len(close_failures) == 1
            assert close_failures[0].get("error_type") == expected_error_type
            # The timeout arm's bound cancels the close before its own
            # unsubscribe (the deferral the real iterator's finally has too);
            # the runtime_error arm's close unsubscribed exactly once.
            await _assert_fake_settled(
                listener, harness, pump, unsubscribes=0 if mode == "timeout" else 1
            )
            assert not loop_exceptions.never_retrieved()
    finally:
        await _drained()
        loop_exceptions.restore()
        await harness.cleanup()


async def test_c9_attached_stop_failure_wins_over_body_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C9 — with an active body error, a successful frame close, and a
    failing ``listener.stop()`` on the attached path, the exact stop
    exception wins, the stop is attempted exactly once, and the
    semaphore/metric still close exactly once — no fake successful
    teardown."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    listener.frames_obj.items.put_nowait(RuntimeError("body failed: c9"))
    listener.stop_error = RuntimeError("stop failed: c9")
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        with pytest.raises(RuntimeError, match="stop failed: c9"):
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


async def test_c11a_body_base_exception_still_stops_listener(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C11a — a custom non-``Exception`` ``BaseException`` from the
    body/iterator propagates unchanged and the teardown still stops the
    listener and settles every resource (no blanket swallow anywhere)."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        listener.frames_obj.items.put_nowait(FatalError("fatal body: c11a"))
        with pytest.raises(FatalError, match="fatal body: c11a"):
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


async def test_c12_finite_iterator_ordinary_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C12 — ordinary exhaustion of a finite iterator ends the consumer with
    ``StopAsyncIteration`` — never a fabricated cancellation — and every
    resource settles exactly once."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        listener.frames_obj.exhausted = True
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


@pytest.mark.parametrize("passes", [0, 2], ids=["initial_rejection", "midstream_revocation"])
async def test_c13_session_rejection_paths(monkeypatch: pytest.MonkeyPatch, passes: int) -> None:
    """C13 — the initial session rejection ends the stream before any REAL
    listener exists (no stop, no release; the fake harness listener object
    exists but its start was never called and nothing real is fabricated);
    the
    midstream revocation after a delivered frame still stops the listener
    and settles every resource — semaphore/metric exactly once on both."""
    harness = _HoldsHarness(monkeypatch, verifier=_RevokingVerifier(passes))
    listener = harness.listener
    try:
        await harness.sem.acquire()
        gen = _holds_generator(harness.sem, _untouched_pool, "taskq", harness.verifier)
        harness.gen = gen
        if passes == 0:
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(gen.__anext__(), timeout=2.0)
            assert listener.start_calls == 0
            assert listener.stop_calls == 0
            assert listener.frames_obj.unsubscribes == 0
            assert listener.pool.releases == []
            await _bounded(lambda: not _pending_others())
        else:
            first = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
            assert first == ": keepalive\n\n"
            pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
            assert pump is not None and not pump.done()
            listener.frames_obj.items.put_nowait(None)  # one more tick, then revoke
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(gen.__anext__(), timeout=2.0)
            await _assert_fake_settled(listener, harness, pump)
        assert _permits(harness.sem) == 1
        assert len(harness.metric_closes) == 1
    finally:
        await harness.cleanup()


async def test_c14_start_failure_still_closes_and_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C14 — a failing ``listener.start()`` retains its error as the
    consumer's outcome (no stop error injected), the frames close and the
    stop are still attempted, no pump is ever created, and the outer
    semaphore/metric close exactly once."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    listener.start_error = RuntimeError("start failed: c14")
    try:
        await harness.sem.acquire()
        gen = _holds_generator(harness.sem, _untouched_pool, "taskq")
        harness.gen = gen
        with pytest.raises(RuntimeError, match="start failed: c14"):
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        assert listener.frames_obj.close_started.is_set()
        assert listener.stop_calls == 1
        assert listener.frames_obj.unsubscribes == 0  # the iterator never ran
        assert listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert listener.pool.acquired == []  # no connection was ever fabricated
        assert _permits(harness.sem) == 1
        assert len(harness.metric_closes) == 1
        await _bounded(lambda: not _pending_others())
    finally:
        await harness.cleanup()


async def test_c15_second_stop_after_settled_close_is_inert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C15 — after the ordinary C1 settlement, an explicit second
    ``listener.stop()`` neither re-cancels nor re-reaps the settled pump,
    never re-releases the connection, and never spawns a new pump; the first
    stop's counts stand (the stop count difference is this control's own
    local block)."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        await gen.aclose()
        await _assert_fake_settled(listener, harness, pump)  # the first stop: exactly one
        await listener.stop()  # the explicit second stop
        assert listener.stop_calls == 2
        assert listener._pump is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump.done()
        assert listener.start_calls == 1  # no new pump spawn
        assert listener.frames_obj.unsubscribes == 1
        assert listener.pool.releases == [listener.pool.conn]  # still exactly one
        assert _permits(harness.sem) == 1
        assert len(harness.metric_closes) == 1
        await _bounded(lambda: not _pending_others())
    finally:
        await harness.cleanup()


@pytest.mark.parametrize("mode", ["pool_none", "schema_none"], ids=["pool_none", "schema_none"])
async def test_c16_no_pool_degraded_keepalive(monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    """C16 — with no pool or no schema the keepalive-only stream never
    fabricates a listener or a connection, the keepalive behavior is
    unchanged, and the exit releases the semaphore/metric exactly once."""
    monkeypatch.setattr(sse_mod, "_KEEPALIVE_INTERVAL", 0.05)
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    resolve: Callable[[], asyncpg.Pool | None] = (
        (lambda: None) if mode == "pool_none" else _untouched_pool
    )
    schema = None if mode == "schema_none" else "taskq"
    try:
        await harness.sem.acquire()
        gen = _holds_generator(harness.sem, resolve, schema)
        harness.gen = gen
        first = await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        assert first == ": keepalive\n\n"
        await gen.aclose()
        assert listener.start_calls == 0  # no listener was ever fabricated
        assert listener.stop_calls == 0
        assert listener.pool.acquired == []  # no connection was ever fabricated
        assert _permits(harness.sem) == 1
        assert len(harness.metric_closes) == 1
        await _bounded(lambda: not _pending_others())
    finally:
        await harness.cleanup()


# ── C17/C18/C19: the real stop bound and the explicitly raised stop ──────
#    exception precedence


@pytest.mark.integration
@pytest.mark.xdist_group(name="listen")
async def test_c17_real_stop_returns_at_bound_release_converges_after_gate(
    monkeypatch: pytest.MonkeyPatch, module_pg_schema: ModulePgSchema
) -> None:
    """C17 — the REAL listener's stop bound: with a dedicated
    ``min_size=1, max_size=1`` real-PostgreSQL pool and the real
    ``_BroadcastListener.stop``/pump/reaper/``release_listen_connection``
    machinery, the bounded stop RETURNS NORMALLY at the bound (no
    ``TimeoutError`` is manufactured) while the pump's shielded release
    cleanup stays detached; after the gate opens the exact connection
    completes its release exactly once and every resource settles.

    Mechanism: the bound's cancellation cascades from the stop's reap await
    into a second pump cancellation; the pump task ends through
    ``_pump_loop``'s own intentional cancellation handling (it catches the
    cancellation and returns after the shielded release detaches), so the
    reaper's await returns normally and ``wait_for`` uncancels the expired
    bound instead of raising. The bound is recorded, never tightened into a
    scheduling contract; the outer deadline here is test containment only.
    No ``uncancel()`` is used; the reaper's behavior and source are
    unmodified — the recorder is ordinary delegating instrumentation (it
    observes entry/exit and forwards to the real reaper)."""
    monkeypatch.setattr(sse_mod, "CLOSE_TIMEOUT_SECS", 0.1)
    dedicated_pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=1)
    spy = _ReleaseSpyPool(dedicated_pool)
    spy.release_gate.clear()  # hold the REAL release phase after the pump's synchronous cancel
    capture = _RealListenerCapture(monkeypatch, gated=False)
    metric_closes = _spy_metric_closes(monkeypatch)
    sem = asyncio.Semaphore(1)
    await sem.acquire()

    # Supporting mechanism evidence: a delegating recorder around the reap
    # helper (the listener's module holds the name), recording entry/exit
    # cancellation accounting without altering its behavior.
    reap_records: list[dict[str, object]] = []
    real_reap = reap_cancelled_child

    async def _recording_reap(task: object) -> None:
        me = asyncio.current_task()
        reap_records.append(
            {
                "phase": "enter",
                "me_cancelling": me.cancelling() if me is not None else None,
                "child_cancelling": task.cancelling(),  # pyright: ignore[reportAttributeAccessIssue]  # Why: the reaper's task is a Task-like with cancelling(); the recorder reads only that.
            }
        )
        try:
            await real_reap(task)  # pyright: ignore[reportArgumentType]  # Why: the reaper accepts any awaitable; the spy forwards the listener's own pump task untouched.
        except asyncio.CancelledError:
            reap_records.append({"phase": "exit", "mode": "reraised"})
            raise
        reap_records.append({"phase": "exit", "mode": "ok"})

    monkeypatch.setattr(listen_pump_mod, "reap_cancelled_child", _recording_reap)

    listener: HitlListener | None = None
    pump: asyncio.Task[None] | None = None
    try:
        gen = await _prime_real_holds_stream(sem, _spied_pool(spy), module_pg_schema.schema_name)
        listener = capture.listeners[0]
        stop_entries = _spy_real_stop(monkeypatch, listener)
        pump = _real_pump(listener)
        assert pump is not None and not pump.done()
        assert pump.get_name() == _PUMP_TASK_NAME
        conn = spy.acquired[0]
        pool_facts = {
            "pool_min_constructed": 1,
            "pool_max": dedicated_pool.get_max_size(),
            "pool_size": dedicated_pool.get_size(),
            "pool_idle": dedicated_pool.get_idle_size(),
        }
        assert dedicated_pool.get_max_size() == 1
        assert dedicated_pool.get_size() - dedicated_pool.get_idle_size() == 1

        # The direct ordinary consumer close, no additional consumer
        # cancellation; the outer wait is containment only.
        started = asyncio.get_running_loop().time()
        await asyncio.wait_for(gen.aclose(), timeout=10.0)
        elapsed = asyncio.get_running_loop().time() - started
        facts = _real_resource_facts(
            listener, pump, spy, dedicated_pool, sem, metric_closes, stop_entries, conn
        )
        facts.update(
            {
                "stop_outcome": "returned",
                "elapsed_secs": elapsed,
                "release_started": spy.release_started.is_set(),
                "release_completions": len(spy.released_completed),
                "detached_cleanup_pending": _pending_others(),
                **pool_facts,
            }
        )
        # The observed normal return at the bound (never a fictional
        # TimeoutError), with the release cleanup started but not completed.
        assert elapsed >= 0.1  # the bound was honored; no tighter contract
        assert len(stop_entries) == 1
        assert _real_pump(listener) is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and pump.done()
        assert (
            not pump.cancelled()
        )  # the pump task ended through its own cancellation handling, normally
        assert spy.release_started.is_set()
        assert len(spy.released_completed) == 0  # started, not completed
        # Exactly ONE task is pending here — the detached shielded cleanup —
        # captured by identity so the post-gate settlement can prove THAT
        # exact task is the one that completed.
        me = asyncio.current_task()
        pending_before_gate = [
            task for task in asyncio.all_tasks() if task is not me and not task.done()
        ]
        assert len(pending_before_gate) == 1
        detached_cleanup = pending_before_gate[0]
        facts["detached_cleanup_task"] = detached_cleanup.get_name()
        _print_diagnostics("c17", facts)
        assert dedicated_pool.get_size() - dedicated_pool.get_idle_size() == 1  # not yet balanced
        assert _permits(sem) == 1  # the consumer itself finished its outer release
        assert len(metric_closes) == 1
        assert [record["phase"] for record in reap_records] == ["enter", "exit"]
        assert (
            reap_records[-1]["mode"] == "ok"
        )  # the reap returned normally; its except path never ran
        assert (
            reap_records[0]["child_cancelling"] == 1
        )  # the pump was cancelled exactly once by stop

        # After the gate: the exact connection completes its release once.
        spy.release_gate.set()
        await _assert_real_settled(
            listener, pump, spy, dedicated_pool, sem, metric_closes, stop_entries, conn
        )
        assert detached_cleanup.done()  # the captured pre-gate pending task is the one that settled
    finally:
        spy.release_gate.set()
        if listener is not None:
            await listener.stop()
        await _drained()
        await dedicated_pool.close()


@pytest.mark.parametrize(
    "stop_exception_factory",
    [
        pytest.param(lambda: TimeoutError("stop bound expired: c18-unique-object"), id="timeout"),
        pytest.param(lambda: FatalError("stop fatal: c19-unique-object"), id="base_exception"),
    ],
)
async def test_c18_c19_explicitly_raised_stop_exception_wins_over_body_error(
    monkeypatch: pytest.MonkeyPatch,
    stop_exception_factory: Callable[[], BaseException],
) -> None:
    """C18/C19 — an explicitly raised stop exception takes attached
    precedence over an already-active body error. The fake stop preserves
    the real ordering (detach slot, synchronously cancel the pump, reap),
    then raises one unique exception object after that first-await/reap
    phase: a ``TimeoutError`` (the C18 arm) or a custom non-``Exception``
    ``BaseException`` (the C19 arm). That exact object — not a copy, not a
    manufactured timeout — escapes to the consumer and supersedes the body
    error, and every resource still settles exactly once; nothing is
    swallowed."""
    harness = _HoldsHarness(monkeypatch)
    listener = harness.listener
    stop_exception = stop_exception_factory()
    label = "c18" if isinstance(stop_exception, TimeoutError) else "c19"
    listener.frames_obj.items.put_nowait(RuntimeError("body failed: c18-c19"))
    listener.stop_error = stop_exception
    try:
        gen = await harness.prime()
        pump = listener._pump  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        assert pump is not None and not pump.done()
        captured: BaseException | None = None
        try:
            await asyncio.wait_for(gen.__anext__(), timeout=2.0)
        except BaseException as exc:  # Why: the C19 arm raises a non-Exception BaseException; the test must catch both arms and pin the exact object identity.
            captured = exc
        assert captured is stop_exception, (
            f"the exact stop exception object must escape and win: got {captured!r}"
        )
        facts = _fake_resource_facts(listener, harness, pump)
        facts["escaping_stop_exception"] = repr(captured)
        facts["escaping_identity_matches"] = captured is stop_exception
        facts["body_error_superseded"] = not isinstance(captured, RuntimeError)
        _print_diagnostics(label, facts)
        await _assert_fake_settled(listener, harness, pump)
    finally:
        await harness.cleanup()


# ── C1-PG / C3-PG / C5-PG: real-listener, real-pump, real-PostgreSQL pins ─


async def _prime_real_holds_stream(
    sem: asyncio.Semaphore,
    resolve: Callable[[], asyncpg.Pool | None],
    schema: str,
) -> AsyncGenerator[str, None]:
    gen = _holds_generator(sem, resolve, schema)
    frame = await asyncio.wait_for(gen.__anext__(), timeout=5.0)
    assert '"event":"backfilled"' in frame, (
        f"the first frame must prove the healthy LISTEN/backfill, got {frame!r}"
    )
    return gen


@pytest.mark.integration
@pytest.mark.xdist_group(name="listen")
async def test_c1_pg_ordinary_disconnect_real_listener_releases_connection(
    monkeypatch: pytest.MonkeyPatch, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """C1-PG — on the REAL listener, REAL pump, and REAL PostgreSQL: an
    ordinary ``gen.aclose()`` (twice — the second is inert) terminates the
    actual named pump, empties the frame registry, returns the exact
    acquired LISTEN connection exactly once, and balances the
    pool/semaphore/metric."""
    capture = _RealListenerCapture(monkeypatch, gated=False)
    metric_closes = _spy_metric_closes(monkeypatch)
    spy = _ReleaseSpyPool(wf_pool)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    listener: HitlListener | None = None
    pump: asyncio.Task[None] | None = None
    try:
        gen = await _prime_real_holds_stream(sem, _spied_pool(spy), wf_schema)
        listener = capture.listeners[0]
        stop_entries = _spy_real_stop(monkeypatch, listener)
        pump = _real_pump(listener)
        assert pump is not None and not pump.done()
        assert pump.get_name() == _PUMP_TASK_NAME
        assert wf_pool.get_size() - wf_pool.get_idle_size() == 1
        conn = spy.acquired[0]
        await gen.aclose()
        await gen.aclose()  # the settled second close: inert
        await _assert_real_settled(
            listener, pump, spy, wf_pool, sem, metric_closes, stop_entries, conn
        )
    finally:
        if listener is not None:
            await listener.stop()
        await _drained()


@pytest.mark.integration
@pytest.mark.xdist_group(name="listen")
async def test_c3_pg_repeat_cancel_real_listener_stops_and_releases(
    monkeypatch: pytest.MonkeyPatch, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """C3-PG — the explicit-asyncio mid-close arm against the REAL listener,
    REAL pump, and REAL migrated PostgreSQL (separate from fake-listener C3
    and never aliased into C4). The wrapped REAL ``frames.aclose()`` is
    gated: the gate is cleared after the real listener/frames are constructed
    and primed, and before the first exit cancellation. The close's entry is
    proven by its ``close_started`` event, and the gate stays held across the
    repeat ``Task.cancel()`` — the close is parked at its gated await
    mid-flight, while the REAL inner generator's own finally has already
    unsubscribed at cancellation-delivery time (the registry is empty while
    the wrapper close gate holds the teardown itself). Stop detaches the pump
    slot before reaping, so
    the slot's state is the direct stop observation, taken before the later
    release/pool-balance/settlement assertions. The bounded stop-entry
    observation after the gate opens is contained (suppressed timeout on
    the skipped-stop side); it never converts the skipped-stop state into a
    timeout failure."""
    capture = _RealListenerCapture(monkeypatch, gated=True)
    metric_closes = _spy_metric_closes(monkeypatch)
    spy = _ReleaseSpyPool(wf_pool)
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    listener: HitlListener | None = None
    pump: asyncio.Task[None] | None = None
    try:
        gen = await _prime_real_holds_stream(sem, _spied_pool(spy), wf_schema)
        listener = capture.listeners[0]
        assert len(capture.gates) == 1  # exactly one gate: the wrapped REAL close
        # The gate is cleared AFTER the real listener/frames are constructed
        # and primed, and BEFORE the first exit cancellation.
        capture.gates[0].close_gate.clear()  # hold the REAL close BEFORE any cancel
        assert not capture.gates[0].close_gate.is_set()
        assert not capture.gates[0].close_started.is_set()
        stop_entries = _spy_real_stop(monkeypatch, listener)
        pump = _real_pump(listener)
        assert pump is not None and not pump.done()
        assert pump.get_name() == _PUMP_TASK_NAME
        assert wf_pool.get_size() - wf_pool.get_idle_size() == 1
        conn = spy.acquired[0]
        stream = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0)
        stream.cancel("initial body cancellation")
        # The close has actually entered its gated await, and the gate is
        # still held at the repeat-cancel instant; both event facts are
        # captured HERE, while the gate is still held (before its release).
        await asyncio.wait_for(capture.gates[0].close_started.wait(), timeout=2.0)
        gate_held_at_repeat_cancel = not capture.gates[0].close_gate.is_set()
        close_started_while_gate_held = capture.gates[0].close_started.is_set()
        subscribers_while_gate_held = _real_subscriber_count(listener)
        assert gate_held_at_repeat_cancel
        assert close_started_while_gate_held
        # The REAL inner generator's own finally unsubscribes at cancel-
        # delivery time (the cancel closes it during the unwind, before the
        # teardown runs), so the registry is already empty while the gated
        # close holds the teardown; the deferred-unsubscribe-until-gate
        # variant is the fake frames' pinned shape (C3). The gated close
        # here holds the teardown's frames-aclose step itself.
        assert subscribers_while_gate_held == 0
        stream.cancel("re-delivered cancellation during frames.aclose")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stream, timeout=2.0)
        capture.gates[0].close_gate.set()  # release the gate only now
        await _bounded(lambda: _real_subscriber_count(listener) == 0)
        # Bounded real-stop-entry observation, contained: on the skipped-stop
        # side this wait expires silently (the pump-slot assertion below
        # stays the primary failure); on the stopping side it proves the
        # detached teardown actually reached stop.
        with contextlib.suppress(TimeoutError):
            await _bounded(lambda: bool(stop_entries), deadline_secs=0.5)
        facts = _real_resource_facts(
            listener, pump, spy, wf_pool, sem, metric_closes, stop_entries, conn
        )
        facts["consumer_cancelled"] = True
        facts["gate_held_at_repeat_cancel"] = gate_held_at_repeat_cancel
        facts["close_started_while_gate_held"] = close_started_while_gate_held
        facts["subscribers_while_gate_held"] = subscribers_while_gate_held
        _print_diagnostics("c3_pg", facts)
        # The REAL listener's stop detaches the pump slot before reaping, so
        # a still-attached slot here is the skipped-stop state itself; the
        # release/pool/settlement assertions below are ordered after it.
        assert _real_pump(listener) is None  # pyright: ignore[reportPrivateUsage]  # Why: the pump slot is the direct stop observation; no public reader.
        await _assert_real_settled(
            listener, pump, spy, wf_pool, sem, metric_closes, stop_entries, conn
        )
    finally:
        if capture.gates:
            capture.gates[0].close_gate.set()
        spy.release_gate.set()
        if listener is not None:
            await listener.stop()
        await _drained()


@pytest.mark.integration
@pytest.mark.xdist_group(name="listen")
async def test_c5_pg_cancel_during_real_pump_release_phase(
    monkeypatch: pytest.MonkeyPatch, wf_pool: asyncpg.Pool, wf_schema: str
) -> None:
    """C5-PG — the real stop cancels its pump synchronously before its first
    await (the pump is already cancelled and still pending when the gated
    release phase starts), and the consumer's re-delivered cancellation
    lands during the REAL pump's shielded release phase; stop, pump
    termination, and the real release all finish after the gate opens,
    exactly once."""
    capture = _RealListenerCapture(monkeypatch, gated=False)
    metric_closes = _spy_metric_closes(monkeypatch)
    spy = _ReleaseSpyPool(wf_pool)
    spy.release_gate.clear()  # hold the REAL pump's release phase
    sem = asyncio.Semaphore(1)
    await sem.acquire()
    listener: HitlListener | None = None
    pump: asyncio.Task[None] | None = None
    try:
        gen = await _prime_real_holds_stream(sem, _spied_pool(spy), wf_schema)
        listener = capture.listeners[0]
        stop_entries = _spy_real_stop(monkeypatch, listener)
        pump = _real_pump(listener)
        assert pump is not None and not pump.done()
        assert wf_pool.get_size() - wf_pool.get_idle_size() == 1
        conn = spy.acquired[0]
        stream = asyncio.create_task(gen.__anext__())
        await asyncio.sleep(0)
        stream.cancel("initial body cancellation")
        await asyncio.wait_for(spy.release_started.wait(), timeout=2.0)
        cancel_count_at_release_start = pump.cancelling()
        assert not pump.done()  # the REAL pump is mid-cleanup, gated
        stream.cancel("re-delivered cancellation during the gated pump release")
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stream, timeout=2.0)
        spy.release_gate.set()
        await _assert_real_settled(
            listener, pump, spy, wf_pool, sem, metric_closes, stop_entries, conn
        )
        assert cancel_count_at_release_start >= 1  # the synchronous cancel came first
    finally:
        spy.release_gate.set()
        if listener is not None:
            await listener.stop()
        await _drained()
