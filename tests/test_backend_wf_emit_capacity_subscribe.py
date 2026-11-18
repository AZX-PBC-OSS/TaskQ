"""The `subscribe_wf_emit_capacity` protocol member's lifecycle controls
(RULING_718_DIFF's L-4 as superseded by correction layer 1's C5).

The capacity notification seam is a REQUIRED `Backend` member beside
`subscribe_leader_wake` (no capability probe remains in the leader — a
missing member is a protocol violation, not a warning arm), and both
built-in backends expose the same typed subscription contract:
entering the context manager registers a fresh `asyncio.Event` in the
backend's own subscriber registry, exiting discards it (no stale
subscribers), and delivery is a hint only — the durable pending intents
and the bounded maintenance selection remain the correctness authority
(no LISTEN is invented in-memory; the PG backend's events are set by the
capacity NOTIFY callback's registry walk).

Mutation notes (each control names what going red proves):
* removing the protocol member reds the protocol inventory gates
  (`tests/test_backend_protocol.py`'s exact 52-member set) and the typed
  call in `worker/leader.py` (pyright);
* removing the in-memory implementation or its registry reds the
  in-memory lifecycle control here;
* removing the subscription cleanup (the context manager's exit
  discard) reds the no-stale-subscriber assertions on both backends.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

import pytest

from taskq.backend._protocol import Backend
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.worker import leader as leader_mod
from tests._ns_patch import patch_ns

pytestmark = pytest.mark.integration


class TestWfEmitCapacitySubscriptionLifecycle:
    """The typed subscription lifecycle on both built-in backends."""

    def test_protocol_member_is_typed_and_documented(self) -> None:
        """The member sits beside its sibling on the protocol with the
        hint-only contract's docstring (the ruled text)."""
        import inspect

        assert hasattr(Backend, "subscribe_wf_emit_capacity")
        assert hasattr(Backend, "subscribe_leader_wake")
        doc = inspect.getdoc(Backend.subscribe_wf_emit_capacity) or ""
        assert "hint" in doc, doc
        assert "correctness authority" in doc, doc

    def test_removal_of_the_warning_arm_is_complete(self) -> None:
        """The deleted capability probe leaves nothing behind: no
        `getattr` probe, no `wf-emit-capacity-subscribe-missing` warning
        kind anywhere in the product (the unpinned warning kind's removal
        reds nothing and is required)."""
        from pathlib import Path

        src = Path("src/taskq")
        for path in src.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            assert "wf-emit-capacity-subscribe-missing" not in text, path
            assert "wf_emit_capacity_subscribe_missing" not in text, path

    async def test_in_memory_subscription_lifecycle(self) -> None:
        """The in-memory backend: enter registers a FRESH event, the
        registry carries it, exit discards it (the sibling's
        subscriber-event/context-manager discipline; no LISTEN
        invented)."""
        from datetime import UTC, datetime

        from taskq.testing.clock import FakeClock
        from taskq.testing.in_memory import InMemoryBackend

        backend = InMemoryBackend(clock=FakeClock(datetime.now(UTC)))
        async with backend.subscribe_wf_emit_capacity() as event:
            assert isinstance(event, asyncio.Event)
            assert not event.is_set()
            assert event in backend._wf_emit_capacity_subscribers
            # The hint discipline: a delivered notification only SETS the
            # event — durability is never carried by it.
            event.set()
            assert event.is_set()
        assert not backend._wf_emit_capacity_subscribers, (
            "a stale subscriber survived the subscription's exit"
        )

    async def test_in_memory_subscription_yields_fresh_events_per_enter(
        self,
    ) -> None:
        """Two concurrent subscriptions carry two distinct events (the
        fan-out shape the leader's and tests' registrations rely on)."""
        from datetime import UTC, datetime

        from taskq.testing.clock import FakeClock
        from taskq.testing.in_memory import InMemoryBackend

        backend = InMemoryBackend(clock=FakeClock(datetime.now(UTC)))
        async with backend.subscribe_wf_emit_capacity() as first:
            async with backend.subscribe_wf_emit_capacity() as second:
                assert first is not second
                assert len(backend._wf_emit_capacity_subscribers) == 2
                second.set()
                assert first.is_set() is False
            assert len(backend._wf_emit_capacity_subscribers) == 1
        assert len(backend._wf_emit_capacity_subscribers) == 0

    async def test_postgres_subscription_lifecycle(
        self,
        module_pg_schema: ModulePgSchema,
        clean_jobs_app: JobsApp,
    ) -> None:
        """The PG backend: the same typed contract — enter registers a
        fresh unset event, exit discards it; the registry is the one the
        capacity NOTIFY callback's walk sets (the notification plumbing
        classification the semantic parity registry records)."""
        backend = clean_jobs_app.backend
        registry = getattr(backend, "_wf_emit_capacity_subscribers", None)
        assert registry is not None, "the PG backend lost its capacity subscriber registry"
        async with backend.subscribe_wf_emit_capacity() as event:
            assert isinstance(event, asyncio.Event)
            assert not event.is_set()
            assert event in registry
            # THE CALLBACK WALK: the capacity NOTIFY callback's own
            # registry walk (worker/notify.py) sets every subscriber —
            # the exact production delivery path, driven by hand here
            # (no LISTEN wire is needed to prove the registration face).
            for sub in list(registry):
                sub.set()
            assert event.is_set()
        assert event not in registry, "a stale subscriber survived the subscription's exit"

    async def test_leader_loop_uses_the_typed_member_directly(
        self,
        module_pg_schema: ModulePgSchema,
        clean_jobs_app: JobsApp,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The maintenance leader's drain loop subscribes through the
        typed member (no getattr probe, no cast) and still runs its
        leadership-gated initial reconciliation + the event-driven drain —
        the periodic fallback is preserved (the sweep table's own
        pending-intent selection is a separate arm; a failing drain
        resumes waiting). The loop's own waiter tasks never outlive the
        loop: every task created while the loop ran is DONE at the end
        (a leaked ``Event.wait`` would still be pending here)."""
        from taskq.worker.leader import MaintenanceLeader

        backend = clean_jobs_app.backend
        drained: list[str] = []

        async def _fake_drain_once() -> None:
            drained.append("once")

        leader = MaintenanceLeader.__new__(MaintenanceLeader)
        leader._backend = backend  # type: ignore[attr-defined]  # Why: the control drives the loop's own subscription face; the leader's real __init__ owns the full deps graph this control does not need.
        leader._wf_emit_capacity_drain_once = _fake_drain_once  # type: ignore[method-assign]  # Why: the drain-once seam is the loop's own arm; the control asserts the subscription + event discipline, not the SQL.
        shutdown = asyncio.Event()

        # THE RESIDUAL-TASK PROBE: record every task created while the
        # loop ran; at the end each must be done (the loop's own finally
        # cancels and awaits its waiter pair). The probe patches the
        # OWNER's own ``asyncio`` binding (taskq.worker.leader's - the
        # module whose code resolves the name), never the stdlib module
        # object: a process-global create_task patch is visible to every
        # coroutine in the process for the patch's length (the suite
        # hygiene control's own violation class) - tests/_ns_patch.py.
        recorded: list[asyncio.Task[object]] = []
        real_create_task = asyncio.create_task

        def _recording_create_task(
            coro: Coroutine[Any, Any, object], **kwargs: Any
        ) -> asyncio.Task[object]:
            task = real_create_task(coro, **kwargs)
            recorded.append(task)
            return task

        patch_ns(monkeypatch, leader_mod, "asyncio", create_task=_recording_create_task)

        async def _notify_and_stop() -> None:
            # Wait for the loop's own registration (the handshake — never
            # a sleep-based race), then deliver: every registered event is
            # set (the callback walk's face), the loop's own drain arm
            # runs again; then the stop arrives.
            while not backend._wf_emit_capacity_subscribers:  # noqa: ASYNC110  # Why: bounded registration poll - the registry fills on the loop task's own enter, not a signal this task can await.
                await asyncio.sleep(0.01)
            registry = backend._wf_emit_capacity_subscribers
            for sub in list(registry):
                sub.set()
            await asyncio.sleep(0.05)
            shutdown.set()

        stopper = asyncio.create_task(_notify_and_stop())
        loop_task = asyncio.create_task(leader._wf_emit_capacity_drain_loop(shutdown))
        await asyncio.wait_for(asyncio.shield(loop_task), timeout=10)
        await stopper
        # The initial reconciliation (leadership assumption's R term) ran,
        # then the event-driven arm ran again on the delivered event.
        assert drained == ["once", "once"], drained
        # THE RESIDUAL-TASK LAW: the loop's own waiters never outlive the
        # loop's frames (the finally's cancel-and-await); a leaked
        # Event.wait task would still be pending here.
        residual = [task for task in recorded if not task.done()]
        assert not residual, residual
