"""The single construction point for every Redis client TaskQ builds itself.

The redis exposure audit found all internal clients built with bare
``redis_async.from_url(...)`` plus the fork-guard connection class: no
health checks, no TCP keepalive, no retry. The exposed surface was
``TaskQ.stream()``/``JobHandle.progress_stream``'s pubsub — an idle-closed
broker connection raised a raw ``redis.ConnectionError`` into the user's
``async for`` (redis-py 8.1's internal PubSub retry is ``retries=0`` unless
the client passes one); the fail-closed surfaces (rate limiting, the
worker's terminal publish, the admin UI) were safe but noisy, every blip
surfacing as a WARNING and a limiter retry budget.

Every client TaskQ constructs from ``TASKQ_REDIS_URL`` (or an equivalent
``redis_url=`` setting) comes from :func:`build_redis_client`, which layers
the resilience defaults over the fork-guard:

- ``health_check_interval=30`` — a connection idle 30s gets a ``PING``
  before reuse, so a broker-side idle drop is detected and healed instead
  of failing the next real command (and a pubsub connection's blocked read
  is covered by the retry arm below).
- ``socket_keepalive=True`` — TCP keepalive probes hold the socket through
  stateful middleboxes and NAT gateways that silently reap idle TCP.
- ``retry`` — :class:`~redis.asyncio.retry.Retry` with
  :class:`~redis.backoff.ExponentialBackoff` (0.05s base, 1.0s cap, 3
  attempts) and ``retry_on_error=[ConnectionError, TimeoutError]``. The
  retry is inherited by EVERY connection the pool hands out — including a
  pubsub connection, whose blocked ``get_message`` read reconnects and
  re-subscribes (via redis-py's ``on_connect`` callback) instead of
  escaping the kill/idle-drop into the consumer.

Deliberately NOT set here: ``socket_timeout``. Every redis wait in this
codebase is bounded by an application-level ``asyncio.wait_for`` (the
reload factory budget, the open/initialize budget, the close budget), and
a global socket timeout would change pubsub blocking-read semantics —
``PubSub.parse_response`` hands ``math.inf`` to ``read_response`` when
``block=True`` and expects the socket to have no client-side timeout. The
retry's ``TimeoutError`` member covers the bounded-wait surfaces without
one.

These are defaults, not policy: caller-supplied clients (``redis_client=``,
``redis_client_factory=``/``WorkerConnections.redis_client_factory``, the
credential-provider factories in ``taskq.auth``) are the caller's to build
— see the note in ``docs/guides/managed-identities.md`` for the kwargs an
Azure deployment should set on its own factory-built clients.
"""

from __future__ import annotations

from typing import Any

from taskq._forkguard import guarded_redis_connection_class

HEALTH_CHECK_INTERVAL_SECS: float = 30.0
"""Idle seconds before a TaskQ-built client health-checks a connection."""

RETRY_RETRIES: int = 3
"""Retry attempts for transient connection errors on TaskQ-built clients."""

RETRY_BACKOFF_CAP_SECS: float = 1.0
RETRY_BACKOFF_BASE_SECS: float = 0.05


def build_redis_client(url: str, *, decode_responses: bool = False) -> Any:
    """Build the one redis client shape every TaskQ-internal surface uses.

    ``redis.asyncio.from_url`` over *url* with the fork-guard connection
    class (a forked child's command write is refused before a byte reaches
    the inherited socket) plus the resilience defaults: health checks every
    30s of idleness, TCP keepalive, and a 3-attempt exponential-backoff
    retry for connection errors and socket timeouts. NO ``socket_timeout``
    is set — see the module docstring for why (app-level bounds everywhere;
    a global one would change pubsub blocking-read semantics).

    A fresh ``Retry`` instance is built per call: ``Retry``/backoff objects
    carry per-attempt state and must never be shared between clients.

    ``decode_responses`` keeps every construction site's explicit bytes mode
    (raw bytes are safer for binary payloads and cluster-safe across
    shards); the admin UI passes ``False`` explicitly as before.
    """
    # Call-time import: the [redis] extra is optional at the module level,
    # the same discipline every other construction site follows.
    from redis.asyncio import from_url
    from redis.asyncio.retry import Retry
    from redis.backoff import ExponentialBackoff
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    return from_url(
        url,
        decode_responses=decode_responses,
        connection_class=guarded_redis_connection_class(),
        health_check_interval=HEALTH_CHECK_INTERVAL_SECS,
        socket_keepalive=True,
        retry=Retry(
            ExponentialBackoff(cap=RETRY_BACKOFF_CAP_SECS, base=RETRY_BACKOFF_BASE_SECS),
            retries=RETRY_RETRIES,
        ),
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
    )
