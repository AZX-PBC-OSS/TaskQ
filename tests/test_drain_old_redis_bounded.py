"""The background Redis drain must be bounded, including its failure path.

`_drain_old_redis` retried `aclose()` with NO timeout after the bounded attempt
had already timed out:

    except TimeoutError:
        logger.warning("redis-drain-timeout", ...)
        with suppress(Exception):
            await client.aclose()      # unbounded

`suppress` catches exceptions; it cannot bound duration. The first `aclose()`
times out precisely because the socket is wedged -- an Azure Cache failover
during an Entra token rotation leaves a half-dead connection -- so the retry
hangs on exactly the condition that produced the timeout. The task is
fire-and-forget in a module-level set, so this leaked one task plus one unclosed
socket per SIGHUP credential rotation, for the life of the process. It never
blocked the reload or shutdown, which is what kept it invisible.
"""

from __future__ import annotations

import asyncio

import pytest

from taskq.worker import deps as deps_mod


class _WedgedRedis:
    """A client whose aclose() never returns -- a half-dead socket."""

    def __init__(self) -> None:
        self.aclose_calls = 0

    async def aclose(self) -> None:
        self.aclose_calls += 1
        await asyncio.Event().wait()  # never completes


class _SlowThenFineRedis:
    def __init__(self) -> None:
        self.aclose_calls = 0

    async def aclose(self) -> None:
        self.aclose_calls += 1
        await asyncio.sleep(0)


async def test_wedged_client_does_not_hang_the_drain_task() -> None:
    """The drain task must complete even when aclose() never returns.

    Pre-fix the task hung forever on the unbounded retry, so this assertion
    would time out rather than fail.
    """
    client = _WedgedRedis()
    deps_mod._drain_tasks.clear()  # Why: module-global registry; isolate this test.

    deps_mod._drain_old_redis(client, 0.05)  # Why: exercising the real drain helper.
    task = next(iter(deps_mod._drain_tasks))

    async with asyncio.timeout(5):
        await task

    assert task.done()
    assert not task.cancelled()
    # Exactly one attempt: the whole point is that it gives up rather than
    # retrying into the same wedged socket.
    assert client.aclose_calls == 1


async def test_drain_task_is_discarded_so_nothing_leaks() -> None:
    client = _SlowThenFineRedis()
    deps_mod._drain_tasks.clear()

    deps_mod._drain_old_redis(client, 1.0)
    task = next(iter(deps_mod._drain_tasks))
    await task
    await asyncio.sleep(0)  # let the done-callback run

    assert deps_mod._drain_tasks == set()
    assert client.aclose_calls == 1


async def test_repeated_rotations_do_not_accumulate_tasks() -> None:
    """One leaked task+socket per rotation was the actual damage profile."""
    deps_mod._drain_tasks.clear()

    clients = [_WedgedRedis() for _ in range(5)]
    for c in clients:
        deps_mod._drain_old_redis(c, 0.02)

    tasks = list(deps_mod._drain_tasks)
    async with asyncio.timeout(5):
        await asyncio.gather(*tasks)
    await asyncio.sleep(0)

    assert deps_mod._drain_tasks == set(), "drain tasks accumulated across rotations"
    assert all(c.aclose_calls == 1 for c in clients)


async def test_drain_never_raises_into_the_background_task() -> None:
    """A raising aclose() must not surface as an unretrieved task exception."""

    class _Boom:
        async def aclose(self) -> None:
            raise RuntimeError("connection reset")

    deps_mod._drain_tasks.clear()
    deps_mod._drain_old_redis(_Boom(), 1.0)
    task = next(iter(deps_mod._drain_tasks))
    await task
    assert task.exception() is None


# ── The siblings: bounded because terminate() is ────────────────────────
#
# `_drain_old_pool` and `_drain_old_conn` were already safe, and that is the
# context for the redis fix: they answer a timed-out graceful close with
# `terminate()`, which is immediate and non-blocking. Redis has no equivalent,
# which is why a retry was reached for there instead. Driving them with a
# wedged close is what proves the timeout path is actually bounded — the
# earlier `"terminate()" in inspect.getsource(...)` check would have passed on
# a `terminate()` call sitting in an unreachable branch.
#
# (The redis helper's own "bounded, one attempt, no hand-rolled retry" claim
# needs no separate source scan: test_wedged_client_does_not_hang_the_drain_task
# above hangs forever if the unbounded retry returns, and its
# `aclose_calls == 1` fails if a second close is bolted on.)


class _WedgedPool:
    """A pool whose close() never returns — a stuck connection holder."""

    def __init__(self) -> None:
        self.terminate_calls = 0

    async def close(self) -> None:
        await asyncio.Event().wait()  # never completes

    def terminate(self) -> None:
        self.terminate_calls += 1


class _WedgedConn:
    """A connection whose close() never returns — a half-dead socket."""

    def __init__(self) -> None:
        self.terminate_calls = 0

    async def close(self) -> None:
        await asyncio.Event().wait()  # never completes

    def terminate(self) -> None:
        self.terminate_calls += 1


@pytest.mark.parametrize(
    ("helper", "wedged"),
    [("_drain_old_pool", _WedgedPool), ("_drain_old_conn", _WedgedConn)],
)
async def test_sibling_drains_terminate_a_close_that_never_returns(
    helper: str, wedged: type[_WedgedPool] | type[_WedgedConn]
) -> None:
    """A graceful close that never returns must not leave the drain task
    hanging: past the timeout the resource is terminated and the task ends.
    Unbounded, this leaks one task plus one old-credential session per
    rotation, for the life of the process."""
    target = wedged()
    deps_mod._drain_tasks.clear()  # Why: module-global registry; isolate this test.

    getattr(deps_mod, helper)(target, "test-label", 0.05)
    task = next(iter(deps_mod._drain_tasks))

    async with asyncio.timeout(5):
        await task

    assert task.done()
    assert not task.cancelled()
    assert target.terminate_calls == 1, (
        "a close() that never returns must be answered by terminate()"
    )
