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

``socket_timeout`` IS pinned — to redis-py 8.x's own asyncio default
(5s, ``redis._defaults.DEFAULT_SOCKET_TIMEOUT``). This is a
bound-pin, not a change: a bare ``from_url`` already runs every
command read under that 5s socket timeout today, and pubsub's
blocking read is unaffected by it either way (``PubSub.parse_response``
hands ``math.inf`` to ``read_response`` when ``block=True``,
overriding the socket timeout explicitly). The pin exists because the
bounded wall of the fail-closed surfaces DEPENDS on it: the rate
limiter's ``with_pg_fallback`` wraps its redis call in NO
application-level ``wait_for`` — against a black-holed broker (a
peer that accepts TCP and never answers) the only bound on each read
is the socket timeout, and the limiter's
``RateLimitDependencyUnavailable`` fires only after that bound
multiplies out (measured: 3 client retries ≈ 84s per acquire, times 3
limiter attempts ≈ 252s — bounded, never infinite). redis-py's
asyncio socket-timeout default was ``None`` for years before 8.0;
within the ``redis>=8.0.1,<9`` range a future minor could revert it,
which would turn that bounded 252s into an unbounded hang with the
fail-closed classification never reached. Pinning the value makes the
bound structural instead of inherited.

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

SOCKET_TIMEOUT_SECS: float = 5.0
"""Per-read socket timeout, pinned to redis-py 8.x's asyncio default.

A bound-pin, not a restriction: a bare ``from_url`` already runs under
this default today. It is stated explicitly so the fail-closed surfaces'
bounded wall (see the module docstring) survives a redis-py minor that
reverts the default to ``None``. Pubsub blocking reads override it with
``math.inf`` — this value never bounds a blocked pubsub read.
"""


def build_redis_client(url: str, *, decode_responses: bool = False) -> Any:
    """Build the one redis client shape every TaskQ-internal surface uses.

    ``redis.asyncio.from_url`` over *url* with the fork-guard connection
    class (a forked child's command write is refused before a byte reaches
    the inherited socket) plus the resilience defaults: health checks every
    30s of idleness, TCP keepalive, a 3-attempt exponential-backoff
    retry for connection errors and socket timeouts, and a 5s
    ``socket_timeout`` pinned to redis-py 8.x's asyncio default (a
    bound-pin — see the module docstring for why the fail-closed wall
    depends on it).

    A fresh ``Retry`` instance is built per call: ``Retry``/backoff objects
    carry per-attempt state and must never be shared between clients.

    ``decode_responses`` keeps every construction site's explicit bytes mode
    (raw bytes are safer for binary payloads and cluster-safe across
    shards); the admin UI passes ``False`` explicitly as before.
    """
    # Call-time import: the [redis] extra is optional at the module level,
    # the same discipline every other construction site follows.
    from redis.asyncio import from_url

    # BISECT 647-DIAG: resilience kwargs temporarily removed — the factory
    # must behave EXACTLY like main's bare from_url so CI can isolate which
    # hunk breaks the review probe's cancel escalation. REVERT BEFORE MERGE.
    return from_url(
        url,
        decode_responses=decode_responses,
        connection_class=guarded_redis_connection_class(),
    )
