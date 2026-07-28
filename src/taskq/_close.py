"""Bounded graceful-close helpers shared by worker, client, and CLI teardown.

Leaf module by design: imports nothing from taskq's worker/client/cli/
migrate packages, so every layer can depend on it without the client→worker
layering inversion (worker modules already import taskq.client._enqueuer)
and without a deps↔shutdown module cycle.
"""

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

from taskq.obs import get_logger

if TYPE_CHECKING:
    # Annotation-only imports: keep this leaf import-weight-free (taskq.testing
    # pins that importing it must not transitively import asyncpg).
    import asyncpg
    import redis.asyncio as redis_async

__all__ = [
    "CLOSE_TIMEOUT_SECS",
    "close_conn_bounded",
    "close_pool_bounded",
    "close_redis_bounded",
]

logger = get_logger(__name__)

# Bounds every TaskQ-initiated graceful close (pools, dedicated conns,
# redis) on teardown AND mid-run error paths. The bound is PER-RESOURCE and
# exit stacks unwind SEQUENTIALLY, so the worst-case teardown tail is
# 6 closes x 5s + 2s publish drain ≈ 32s against a fully dead PG/Redis.
# Stacked on the shutdown phase graces that can exceed a 60s Kubernetes
# terminationGracePeriodSeconds, and the settings validator's grace margin
# does not account for this tail — size pod grace budgets accordingly (or
# derive the bound from the grace budget in future).
CLOSE_TIMEOUT_SECS: float = 5.0


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


async def close_redis_bounded(client: "redis_async.Redis", close_timeout: float) -> None:  # type: ignore[type-arg]  # Why: redis_async is under TYPE_CHECKING; string annotation avoids runtime import. type-arg: redis-py stubs expose Redis as an unparameterised generic.
    """Close a Redis client during teardown, bounded by ``close_timeout``.

    On timeout log-and-continue (Redis has no ``terminate()``); any other
    error is logged and swallowed so teardown keeps unwinding. Never raises
    — ``CancelledError`` (a ``BaseException``) still propagates.
    """
    try:
        await asyncio.wait_for(client.aclose(), timeout=close_timeout)
    except TimeoutError:
        logger.warning("redis-teardown-close-timeout", close_timeout=close_timeout)
    except Exception as exc:
        logger.warning("redis-teardown-close-error", error=repr(exc))
