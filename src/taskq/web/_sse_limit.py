"""Shared connection cap for Server-Sent Events endpoints.

Every SSE connection pins server resources for as long as the client keeps it
open: an asyncio task, a Postgres LISTEN connection or a Redis pubsub
subscription, and a socket/file descriptor. Without a cap, any principal who
can reach the route can open them until the process runs out -- on the app
hosting the ingestion pipeline, not on some isolated dashboard tier.

`web/admin/sse.py` caps its own `/sse/{topic}` endpoint this way; the
per-job progress stream (``/jobs/api/job/{job_id}/progress/stream``, which
holds a Redis pubsub subscription) shares the mechanism from here rather
than duplicating it.

The semaphore is keyed by endpoint family and limit, and is process-local.
Multiple worker processes each get their own budget, which is the same
semantics the admin endpoint already had.

The module also owns the SSE session re-check (``session_gate``): the
mid-stream verifier bound every streaming generator shares, beside the
``SESSION_RECHECK_TIMEOUT_SECS`` constant that bounds it.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping

import structlog
from fastapi import HTTPException

from taskq.obs import (
    SseSurface,
    record_sse_connection_closed,
    record_sse_connection_opened,
    record_sse_rejection,
)

__all__ = [
    "SESSION_RECHECK_TIMEOUT_SECS",
    "acquire_sse_slot",
    "release_after",
    "session_gate",
]

#: The progress stream's one endpoint-family key (the constant
#: ``web/progress.py`` passes ``acquire_sse_slot``); also the default
#: ``release_after`` topic. The gauge's closed topic vocabulary keeps
#: this a constant, never caller text.
_PROGRESS_TOPIC = "progress-stream"

_SEMAPHORES: dict[tuple[str, int], asyncio.Semaphore] = {}

#: Non-blocking acquire. A waiting client would hold the request open while
#: queueing for a slot, which is the resource exhaustion being prevented.
_ACQUIRE_TIMEOUT: float = 0.001

#: Bound on one SSE session re-check invocation . The re-check is a
#: host-supplied async callable; taskq's own verifiers are pure cookie/token
#: comparisons that cannot suspend, but a host verifier that awaits a wedged
#: IdP introspection endpoint would otherwise freeze the streaming generator
#: inside its own keepalive path -- the stream never ends, and the SSE slot
#: and the Redis subscription stay pinned for as long as the verifier hangs.
#: A check that outlives this bound is treated as revocation (fail closed):
#: unknown session state must not keep a privileged stream open.
SESSION_RECHECK_TIMEOUT_SECS: float = 5.0


def session_gate(
    session_verifier: Callable[[], Awaitable[bool]] | None,
    log: structlog.stdlib.BoundLogger,
    *,
    timeout_warning: str,
    timeout_fields: Mapping[str, object],
) -> Callable[[], Awaitable[bool]]:
    """The mid-stream session re-check, built once per stream: runs
    before the first frame and once per loop iteration — before every
    yielded event and at every keepalive tick. A failure ends the
    stream (fail closed); a verifier exception is revocation; a check
    that outlives :data:`SESSION_RECHECK_TIMEOUT_SECS` is unknown
    state, also revocation (a hung host verifier must not freeze the
    stream inside its own keepalive path).

    Every SSE generator that re-checks a session rides this one gate;
    *log*, *timeout_warning* and *timeout_fields* carry the generator's
    own surface — its logger and the timeout incident's event name and
    static fields (the job id, the channel, the topic, the run id), so
    each stream's timeout reads under its own vocabulary. The bound and
    the stream phase ride every timeout event the helper logs.
    """
    recheck_count = {"n": 0}

    async def _session_still_valid() -> bool:
        if session_verifier is None:
            return True
        first_check = recheck_count["n"] == 0
        try:
            recheck_count["n"] += 1
            return bool(
                await asyncio.wait_for(
                    session_verifier(),
                    timeout=SESSION_RECHECK_TIMEOUT_SECS,
                )
            )
        except TimeoutError:
            # A verifier that outlives its bound is an unknown session
            # state, not a pass: fail closed, and say why -- a hung
            # verifier (wedged IdP introspection call) wedging the
            # stream is a different incident from a revoked session.
            log.warning(
                timeout_warning,
                **timeout_fields,
                timeout_secs=SESSION_RECHECK_TIMEOUT_SECS,
                stream_phase="initial" if first_check else "streaming",
            )
            return False
        except Exception:
            # Fail closed: an unknown session state must not keep a
            # privileged stream open. The generator names the
            # revocation on its own surface.
            return False

    return _session_still_valid


def _semaphore(key: str, limit: int) -> asyncio.Semaphore:
    # The limit is part of the key because one process can mount the same
    # family at several limits (the test suite does; a misconfigured process
    # could), and a later mount must enforce the limit it was given, keying
    # by family alone made the first mount's limit silently govern every
    # later one. Mounts that agree on the limit still share one budget,
    # which is the cross-router sharing the family key exists to provide.
    scoped = (key, limit)
    if scoped not in _SEMAPHORES:
        _SEMAPHORES[scoped] = asyncio.Semaphore(limit)
    return _SEMAPHORES[scoped]


async def acquire_sse_slot(
    key: str, limit: int, *, surface: SseSurface = "progress"
) -> asyncio.Semaphore:
    """Take an SSE slot for *key* or raise 429.

    Returns the semaphore so the caller can release it when its stream ends.
    Callers MUST release exactly once, in a ``finally`` inside the streaming
    generator -- releasing in the route handler would free the slot while the
    stream is still open. *surface* names the endpoint family for the SSE
    gauges (the closed SseSurface vocabulary); the default keeps the
    original progress-stream callers' labels.

    Instrumentation (the progress surface's SSE health): the rejection is
    counted AT the 429 (one of the two 429 sites; the admin topic endpoint
    raises its own), and the level gauge moves at the same sites the slot
    itself moves — the caller's acquire-success here counts +1 via
    ``record_sse_connection_opened`` and the generator's release counts it
    back. ``key`` is the endpoint family constant ("progress-stream"), the
    closed topic vocabulary the connection gauge's description names —
    never caller-supplied text, so the series set stays bounded.
    """
    semaphore = _semaphore(key, limit)
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=_ACQUIRE_TIMEOUT)
    except TimeoutError:
        record_sse_rejection(surface, key)
        raise HTTPException(
            status_code=429,
            detail=f"too many concurrent SSE connections for {key!r}",
        ) from None
    record_sse_connection_opened(surface, key)
    return semaphore


async def release_after[GenT](
    semaphore: asyncio.Semaphore,
    gen: AsyncGenerator[GenT, None],
    topic: str = _PROGRESS_TOPIC,
    *,
    surface: SseSurface = "progress",
) -> AsyncGenerator[GenT, None]:
    """Wrap *gen*, releasing *semaphore* when it finishes for any reason.

    Client disconnect surfaces as ``CancelledError`` thrown into the
    generator, so the release has to be in a ``finally`` around the
    iteration rather than after it. *topic* must be the family key the
    slot was acquired with, so the connections gauge counts the release
    the semaphore's release performs.
    """
    try:
        # Deterministic close of the wrapped generator on every exit: a
        # disconnect closes this generator (CancelledError at the yield /
        # an explicit aclose), never the inner one, and without the
        # aclosing the ``async for`` abandons *gen* suspended mid-iteration.
        # The inner generator's cleanup then waits on the GC's asyncgen
        # finalizer, which mints an ``async_generator_athrow`` task per
        # abandoned generator - the residue the loop-leak guard names.
        async with contextlib.aclosing(gen):
            async for chunk in gen:
                yield chunk
    finally:
        semaphore.release()
        # The level gauge moves where the slot moves. *topic* must be the
        # family key the slot was acquired with (the gauge's closed
        # vocabulary); the default is the progress stream's own constant,
        # the only production caller's key.
        record_sse_connection_closed(surface, topic)
