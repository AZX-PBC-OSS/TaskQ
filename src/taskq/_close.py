"""Bounded graceful-close helpers shared by worker, client, and CLI teardown.

Leaf module by design: imports nothing from taskq's worker/client/cli/
migrate packages, so every layer can depend on it without the client→worker
layering inversion (worker modules already import taskq.client._enqueuer)
and without a deps↔shutdown module cycle.
"""

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Protocol

from taskq.obs import get_logger

if TYPE_CHECKING:
    # Annotation-only imports: keep this leaf import-weight-free (taskq.testing
    # pins that importing it must not transitively import asyncpg).
    import asyncpg

__all__ = [
    "CLOSE_TIMEOUT_SECS",
    "PUBLISH_DRAIN_TIMEOUT_SECS",
    "close_conn_bounded",
    "close_pool_bounded",
    "close_redis_bounded",
    "worst_case_teardown_tail",
]

logger = get_logger(__name__)

# Bounds every TaskQ-initiated graceful close (pools, dedicated conns,
# redis) on teardown AND mid-run error paths. The bound is PER-RESOURCE and
# exit stacks unwind SEQUENTIALLY, so the per-resource bound multiplies.
# See worst_case_teardown_tail() below for the modelled total and for why
# it is additive on top of the shutdown phase graces rather than inside
# them. WorkerSettings surfaces the consequence at startup
# (`shutdown-budget-exceeds-termination-grace`).
CLOSE_TIMEOUT_SECS: float = 5.0

# Bounded closes that unwind SEQUENTIALLY on the worker's AsyncExitStack:
# 3 pools (dispatcher, heartbeat, worker) + notify_conn + redis_client.
#
# The leader connection is deliberately NOT counted. orchestrate_shutdown
# closes and nulls it before the stack unwinds, so the stack's own
# leader guard sees None and skips -- and that close runs CONCURRENTLY
# with the unwind rather than before it, because the orchestrator task is
# awaited outside the `async with open_worker_deps` block. Counting it
# would overstate the SIGTERM tail by one close.
_SEQUENTIAL_BOUNDED_CLOSES: int = 5

# Bound on the trailing progress-publish drain (asyncio.wait timeout in the
# worker's teardown callback). Additive on top of the closes above.
PUBLISH_DRAIN_TIMEOUT_SECS: float = 2.0


def worst_case_teardown_tail(close_timeout: float = CLOSE_TIMEOUT_SECS) -> float:
    """Modelled worst-case teardown tail, in seconds, against a dead backend.

    This tail is strictly ADDITIVE on top of the shutdown phase graces
    (``cancellation_grace_period`` + ``cleanup_grace_period``): the phases
    run inside the worker's ``open_worker_deps`` context, and this tail is
    the exit-stack unwind that happens after they finish. A deployment
    whose pod grace is sized only from ``termination_grace_period`` is
    therefore under-provisioned by this amount, and gets SIGKILLed
    mid-unwind -- truncating in-flight terminal writes so those jobs are
    recovered later by crash reclaim instead of finalizing cleanly.

    Only reachable against a genuinely dead or hung Postgres/Redis; every
    close returns promptly in the normal case.
    """
    return _SEQUENTIAL_BOUNDED_CLOSES * close_timeout + PUBLISH_DRAIN_TIMEOUT_SECS


async def close_pool_bounded(pool: "asyncpg.Pool", label: str, close_timeout: float) -> None:
    """Close a pool during final teardown, bounded by ``close_timeout``.

    NEVER raises: on timeout the pool is *terminated* — ``close()`` waits
    for checked-out connections to be released, which a dead PG can block
    indefinitely (the CI chaos hang this helper exists to prevent), so
    ``terminate()`` kills them immediately. Any other error is logged and
    swallowed so teardown keeps unwinding. ``CancelledError`` (a
    ``BaseException``) is deliberately not caught, so outer cancellation
    still unwinds promptly. Mirrors the reload path's ``_drain_old_pool``
    in ``taskq.worker.deps``.
    """
    try:
        await asyncio.wait_for(pool.close(), timeout=close_timeout)
    except TimeoutError:
        logger.warning(
            "pool-teardown-close-timeout-terminating", pool=label, close_timeout=close_timeout
        )
        with suppress(Exception):
            pool.terminate()
    except Exception as exc:
        logger.warning("pool-teardown-close-error", pool=label, error=repr(exc))


async def close_conn_bounded(
    conn: "asyncpg.Connection", label: str, close_timeout: float, *, mid_run: bool = False
) -> None:
    """Close a dedicated connection, bounded by ``close_timeout``.

    Same never-raise contract as :func:`close_pool_bounded`: timeout →
    warning log + ``terminate()``; any other error → warning log only.
    ``CancelledError`` propagates. Mirrors the reload path's
    ``_drain_old_conn`` in ``taskq.worker.deps``.

    ``mid_run`` selects the structlog event family: the default
    ``conn-teardown-close-*`` family marks final teardown (where a dead PG
    at shutdown is expected-ish); mid-run callers (leader watchdog/
    election, notify reconnect, isolate-self) pass ``mid_run=True`` for
    the ``conn-close-*`` family so an unexpected mid-run close timeout —
    worker alive, conn so dead that even close() hung — stays
    distinguishable in log alerts. Event names are kept as literals in
    both branches so they remain grep-able.
    """
    try:
        await asyncio.wait_for(conn.close(), timeout=close_timeout)
    except TimeoutError:
        if mid_run:
            logger.warning(
                "conn-close-timeout-terminating", label=label, close_timeout=close_timeout
            )
        else:
            logger.warning(
                "conn-teardown-close-timeout-terminating",
                label=label,
                close_timeout=close_timeout,
            )
        with suppress(Exception):
            conn.terminate()
    except Exception as exc:
        if mid_run:
            logger.warning("conn-close-error", label=label, error=repr(exc))
        else:
            logger.warning("conn-teardown-close-error", label=label, error=repr(exc))


class _AsyncCloseable(Protocol):
    """Structural boundary for Redis resources: an async ``aclose()``.

    Covers ``redis.asyncio.Redis`` clients and ``redis.asyncio.client.PubSub``
    (pub/sub closes are bounded by the same helper) without a runtime import
    of the optional ``[redis]`` extra — this module must stay a leaf.
    """

    async def aclose(self) -> None: ...


async def close_redis_bounded(client: _AsyncCloseable, label: str, close_timeout: float) -> None:
    """Close a Redis client/pubsub during teardown, bounded by ``close_timeout``.

    On timeout log-and-continue (Redis has no ``terminate()``); any other
    error is logged and swallowed so teardown keeps unwinding. Never raises
    — ``CancelledError`` (a ``BaseException``) still propagates. ``label``
    identifies which resource hung/errored and is carried on both log
    events, matching the pool/conn siblings' ``resource, label, timeout``
    signature order.
    """
    try:
        await asyncio.wait_for(client.aclose(), timeout=close_timeout)
    except TimeoutError:
        logger.warning("redis-teardown-close-timeout", label=label, close_timeout=close_timeout)
    except Exception as exc:
        logger.warning("redis-teardown-close-error", label=label, error=repr(exc))
