"""Bounded graceful-close helpers shared by worker, client, and CLI teardown.

Leaf module by design: imports nothing from taskq's worker/client/cli/
migrate packages, so every layer can depend on it without the client→worker
layering inversion (worker modules already import taskq.client._enqueuer)
and without a deps↔shutdown module cycle.
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Literal, Protocol

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
    "close_provider_bounded",
    "close_redis_bounded",
    "worst_case_teardown_tail",
]

logger = get_logger(__name__)

# Which structlog event family a bounded close logs. ``teardown`` marks final
# teardown (default); ``mid_run`` marks an alive-worker emergency close
# (conn only); ``drain`` marks the credential-reload drain path (the
# background-task wrappers in ``taskq.worker.deps``). Event names are kept as
# literals in every branch so they remain grep-able by log alerts.
type CloseEventFamily = Literal["teardown", "mid_run", "drain"]

# Bounds every TaskQ-initiated graceful close (pools, dedicated conns,
# redis) on teardown AND mid-run error paths. The bound is PER-RESOURCE and
# exit stacks unwind SEQUENTIALLY, so the per-resource bound multiplies.
# See worst_case_teardown_tail() below for the modelled total and for why
# it is additive on top of the shutdown phase graces rather than inside
# them. WorkerSettings surfaces the consequence at startup
# (`shutdown-budget-exceeds-termination-grace`).
CLOSE_TIMEOUT_SECS: float = 5.0

# Bounded closes that unwind SEQUENTIALLY on the worker's AsyncExitStack:
# up to 4 pools (dispatcher, heartbeat, worker, and the conditional
# per-slot transaction pool) + notify_conn + redis_client + the two
# credential providers (pg, redis) a provider-backed deployment resolves.
#
# The slot pool is conditional (it exists only when a LOOP-scope
# connection is registered and max_concurrency > 1), but this constant
# models the WORST case, a deployment's SIGTERM budget must not be
# silently short one close timeout because its worker happens to run
# the per-slot path. The providers follow the same rule: a managed-
# identity deployment resolves at most one pg provider and one redis
# provider, and each is closed once, after every resource built through
# it (open_worker_deps pushes them first so they unwind last).
#
# Scope note: this models the BOOT-time stack. Each credential reload
# pushes one more bounded close per factory-backed pool onto the same
# stack, so a worker that has rotated K times unwinds (this count + K)
# sequential closes, a pre-existing property of the reload
# registration pattern, shared by the role pools; size crash budgets on
# rotation-heavy deployments accordingly.
#
# The leader connection is deliberately NOT counted. orchestrate_shutdown
# closes and nulls it before the stack unwinds, so the stack's own
# leader guard sees None and skips -- and that close runs CONCURRENTLY
# with the unwind rather than before it, because the orchestrator task is
# awaited outside the `async with open_worker_deps` block. Counting it
# would overstate the SIGTERM tail by one close.
#
# Caveat, sibling-crash path: when a sibling crash (not SIGTERM) tears
# the worker down (worker/_bootstrap.py's _guarded sets shutdown_event
# with no orchestrator), the early leader close never ran, so the exit
# stack's leader guard closes a TaskQ-owned leader conn sequentially
# too: 9 closes ≈ 47s on that path, not the 42s modelled here. The
# startup warning's number understates the crash path by one close.
_SEQUENTIAL_BOUNDED_CLOSES: int = 8

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
    close returns promptly in the normal case. Counts the conditional
    per-slot pool: the worst case is a worker that has one.

    Sibling-crash caveat: on the path where a sibling crash (not an
    orchestrated shutdown) tears the worker down, the orchestrator's
    early leader-conn close never ran and the exit stack's leader guard
    closes a TaskQ-owned leader conn sequentially as well, nine
    bounded closes, ~47s at the default bound, understated by the 42s
    modelled here.
    """
    return _SEQUENTIAL_BOUNDED_CLOSES * close_timeout + PUBLISH_DRAIN_TIMEOUT_SECS


async def close_pool_bounded(
    pool: "asyncpg.Pool", label: str, close_timeout: float, *, family: CloseEventFamily = "teardown"
) -> None:
    """Close a pool during final teardown, bounded by ``close_timeout``.

    NEVER raises: on timeout the pool is *terminated*, ``close()`` waits
    for checked-out connections to be released, which a dead PG can block
    indefinitely (the CI chaos hang this helper exists to prevent), so
    ``terminate()`` kills them immediately. Any other error is logged and
    swallowed so teardown keeps unwinding. ``CancelledError`` (a
    ``BaseException``) is deliberately not caught, so outer cancellation
    still unwinds promptly. The reload path's ``_drain_old_pool`` in
    ``taskq.worker.deps`` delegates here with ``family="drain"``.

    ``family`` selects the structlog event family: the default
    ``pool-teardown-close-*`` marks final teardown and carries the
    ``close_timeout=`` field; ``family="drain"`` emits the reload path's
    ``pool-drain-*`` family (announced by ``pool-draining`` first) with the
    ``drain_timeout=`` field, so the teardown close bound stays
    distinguishable from the reload drain bound in log alerts. Event names
    are kept as literals in both branches so they remain grep-able.
    """
    if family == "drain":
        logger.info("pool-draining", pool=label, drain_timeout=close_timeout)
    try:
        await asyncio.wait_for(pool.close(), timeout=close_timeout)
    except TimeoutError:
        if family == "drain":
            logger.warning(
                "pool-drain-timeout-terminating", pool=label, drain_timeout=close_timeout
            )
        else:
            logger.warning(
                "pool-teardown-close-timeout-terminating", pool=label, close_timeout=close_timeout
            )
        with suppress(Exception):
            pool.terminate()
    except Exception as exc:
        if family == "drain":
            logger.warning("pool-drain-error", pool=label, error=repr(exc))
        else:
            logger.warning("pool-teardown-close-error", pool=label, error=repr(exc))


async def close_conn_bounded(
    conn: "asyncpg.Connection",
    label: str,
    close_timeout: float,
    *,
    family: CloseEventFamily = "teardown",
) -> None:
    """Close a dedicated connection, bounded by ``close_timeout``.

    Same never-raise contract as :func:`close_pool_bounded`: timeout →
    warning log + ``terminate()``; any other error → warning log only.
    ``CancelledError`` propagates. The reload path's ``_drain_old_conn``
    in ``taskq.worker.deps`` delegates here with ``family="drain"``.

    ``family`` selects the structlog event family: the default
    ``conn-teardown-close-*`` family marks final teardown (where a dead PG
    at shutdown is expected-ish); ``family="mid_run"`` (leader watchdog/
    election, notify reconnect, isolate-self) emits the ``conn-close-*``
    family so an unexpected mid-run close timeout , worker alive, conn so
    dead that even close() hung, stays distinguishable in log alerts;
    ``family="drain"`` emits the reload path's ``conn-drain-*`` family with
    the ``drain_timeout=`` field. Event names are kept as literals in every
    branch so they remain grep-able.
    """
    try:
        await asyncio.wait_for(conn.close(), timeout=close_timeout)
    except TimeoutError:
        if family == "drain":
            logger.warning(
                "conn-drain-timeout-terminating", label=label, drain_timeout=close_timeout
            )
        elif family == "mid_run":
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
        if family == "drain":
            logger.warning("conn-drain-error", label=label, error=repr(exc))
        elif family == "mid_run":
            logger.warning("conn-close-error", label=label, error=repr(exc))
        else:
            logger.warning("conn-teardown-close-error", label=label, error=repr(exc))


class _AsyncCloseable(Protocol):
    """Structural boundary for Redis resources: an async ``aclose()``.

    Covers ``redis.asyncio.Redis`` clients and ``redis.asyncio.client.PubSub``
    (pub/sub closes are bounded by the same helper) without a runtime import
    of the optional ``[redis]`` extra, this module must stay a leaf.
    """

    async def aclose(self) -> None: ...


async def close_redis_bounded(client: _AsyncCloseable, label: str, close_timeout: float) -> None:
    """Close a Redis client/pubsub during teardown, bounded by ``close_timeout``.

     On timeout log-and-continue (Redis has no ``terminate()``); any other
     error is logged and swallowed so teardown keeps unwinding. Never raises
    , ``CancelledError`` (a ``BaseException``) still propagates. ``label``
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


async def close_provider_bounded(provider: object, label: str, close_timeout: float) -> None:
    """Close a credential provider during teardown, bounded by ``close_timeout``.

    A provider that owns a resource - the Entra ID providers' lazily created
    ``DefaultAzureCredential`` holds an aiohttp session - releases it through
    an async ``aclose()``; a provider without one (a token signer, a Vault
    client the caller owns) has nothing to release and is left alone. Same
    log-and-continue contract as :func:`close_redis_bounded`: never raises,
    a hung close is reported and teardown keeps unwinding.
    """
    aclose: Callable[[], Awaitable[None]] | None = getattr(provider, "aclose", None)
    if not callable(aclose):
        return
    try:
        await asyncio.wait_for(aclose(), timeout=close_timeout)
    except TimeoutError:
        logger.warning("provider-teardown-close-timeout", label=label, close_timeout=close_timeout)
    except Exception as exc:
        logger.warning("provider-teardown-close-error", label=label, error=repr(exc))
