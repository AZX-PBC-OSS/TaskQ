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


def test_drain_uses_the_canonical_bounded_close() -> None:
    """No hand-rolled second close routine -- the codebase states the
    'every close is bounded' invariant explicitly elsewhere."""
    import inspect

    src = inspect.getsource(deps_mod._drain_old_redis)
    # Strip the docstring: it deliberately quotes the old buggy code, so a
    # naive substring check would match its own explanation.
    body = src.split('"""')[-1]
    assert "close_redis_bounded(" in body
    assert "suppress(Exception)" not in body, "the unbounded retry is back"
    assert "await client.aclose()" not in body, "hand-rolled close is back"


@pytest.mark.parametrize("helper", ["_drain_old_pool", "_drain_old_conn"])
def test_sibling_drains_bound_their_timeout_path(helper: str) -> None:
    """Context for the fix: the siblings were already safe because they call
    terminate(), which is bounded and non-blocking. Redis has no equivalent,
    which is why a retry was reached for here."""
    import inspect

    src = inspect.getsource(getattr(deps_mod, helper))
    assert "terminate()" in src
