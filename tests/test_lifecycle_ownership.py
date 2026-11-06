"""The lifecycle registry's ownership proofs: the work a test's code
creates is enumerated by the registry (the active scope's leases), not
reconstructed from ``asyncio.all_tasks`` ancestry. Each pin drives one
creation shape from Kimi's list: a task tree whose ancestors finish
first, a plain Future's done callback, and an explicit context that
keeps its ordinary flags while retaining lifecycle ownership."""

from __future__ import annotations

import asyncio
import contextvars

from tests._lifecycle import ResourceLease, current_scope

#: A flag that must survive an explicit-context creation: ownership is
#: carried by the same context and must not displace ordinary values.
_FLAG: contextvars.ContextVar[str] = contextvars.ContextVar("lifecycle-flag")


def _scope_leases() -> list[ResourceLease]:
    scope = current_scope()
    assert scope is not None, "no lifecycle scope is active in the test task"
    return list(scope.leases)


def _names(leases: list[ResourceLease]) -> set[str]:
    return {lease.name for lease in leases}


async def test_task_tree_is_registry_enumerated_when_ancestors_finish_first() -> None:
    """A call-owned task finishes, its done callback creates a child, the
    child finishes, and the child's done callback creates a grandchild:
    every generation is registered in the test scope by the factory, with
    no ancestry reconstruction, because each generation's callback runs
    in its parent task's context and the context carries the scope."""
    created: list[asyncio.Task[object]] = []
    spawned = asyncio.Event()

    async def _leaf() -> None:
        await asyncio.sleep(0.01)

    async def _child() -> None:
        await asyncio.sleep(0.01)
        child = asyncio.get_running_loop().create_task(_leaf(), name="lifecycle-grandchild")
        created.append(child)

    async def _parent() -> None:
        await asyncio.sleep(0.01)

        def _spawn_child(_task: asyncio.Task[object]) -> None:
            child = asyncio.get_running_loop().create_task(_child(), name="lifecycle-child")
            created.append(child)
            spawned.set()

        holder = asyncio.get_running_loop().create_task(asyncio.sleep(0), name="lifecycle-anchor")
        holder.add_done_callback(_spawn_child)

    tree = asyncio.get_running_loop().create_task(_parent(), name="lifecycle-parent")
    await tree
    # The callback is scheduled by the holder's completion and can run
    # after ``await tree`` resumes; wait for it before asserting.
    await asyncio.wait_for(spawned.wait(), timeout=5.0)
    for task in created:
        await asyncio.wait_for(task, timeout=5.0)
    names = _names(_scope_leases())
    assert "lifecycle-parent" in names
    assert "lifecycle-child" in names, f"the callback child is not registered: {names}"
    assert "lifecycle-grandchild" in names, f"the grandchild is not registered: {names}"


async def test_plain_future_callback_child_is_registered() -> None:
    """A plain Future's done callback creates a task: the callback runs in
    the future's captured context, so the factory registers the child in
    the test scope, the same guarantee the Task path gets."""
    created: list[asyncio.Task[object]] = []
    spawned = asyncio.Event()

    async def _leaf() -> None:
        await asyncio.sleep(0.01)

    def _spawn(_fut: asyncio.Future[None]) -> None:
        created.append(asyncio.get_running_loop().create_task(_leaf(), name="lifecycle-fut-child"))
        spawned.set()

    fut: asyncio.Future[None] = asyncio.Future()
    fut.add_done_callback(_spawn)
    fut.set_result(None)
    await asyncio.wait_for(spawned.wait(), timeout=5.0)
    await asyncio.wait_for(created[0], timeout=5.0)
    assert "lifecycle-fut-child" in _names(_scope_leases())


async def test_explicit_context_preserves_flags_and_ownership() -> None:
    """A task created with an explicit context sees its ordinary flag AND
    lands in the test scope: the factory reads the scope from the given
    context, so explicit-context creation is owned, not quarantined."""
    _FLAG.set("on")
    ctx = contextvars.copy_context()

    async def _reader(seen: list[str]) -> None:
        seen.append(_FLAG.get())

    seen: list[str] = []
    task = asyncio.get_running_loop().create_task(
        _reader(seen), context=ctx, name="lifecycle-flagged"
    )
    await asyncio.wait_for(task, timeout=5.0)
    assert seen == ["on"], f"the explicit context lost the flag: {seen}"
    assert "lifecycle-flagged" in _names(_scope_leases())
