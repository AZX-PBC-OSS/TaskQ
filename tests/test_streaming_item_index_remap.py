"""Streaming per-item errors must name STREAM-GLOBAL indices.

``enqueue_batch_streaming``'s no-connection chunked arm commits each
chunk in its own transaction, so a per-item failure on a LATER chunk
leaves every earlier chunk durably stored. Every per-item typed error
that crosses the chunk boundary must therefore name the item's position
in the CALLER's stream: a chunk-local index names a stream position that
was never attempted while the committed prefix stays durable, and a
retry guided by the wrong index duplicates that prefix.

The remapping is a registry at the boundary
(``_ITEM_ERROR_REMAPS`` in ``taskq.client._jobs``) - one dispatch every
remappable per-item error type crosses - not per-exception-type except
clauses at the call site. Any future typed per-item backend error joins
the registry in one place instead of silently inheriting the backend's
chunk-local indices. The tests here drive one of each registry member
through the boundary end-to-end and pin the registry's membership, so
removing an entry - or admitting a per-item error type the boundary does
not remap - fails in this file.

The in-memory backend's batch NUL preflight mirrors the PG build loop's
guard verbatim (the same shared ``item_jsonb_param`` /
``item_tags_jsonb_param`` helpers, the same per-item-annotated
``PayloadValidationError`` with per-call indices), so this whole file
runs with no Docker and no Postgres.
"""

from collections.abc import Iterable
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ValidationError

from taskq import actor
from taskq.batch import EnqueueItem
from taskq.client._jobs import JobsClient
from taskq.exceptions import BatchMaxPendingExceededError, PayloadValidationError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _TextPayload(BaseModel):
    text: str = "ok"


class _ForeignPayload(BaseModel):
    """A payload of the wrong model: passes construction, fails the
    actor's ``payload_type`` validation inside ``build_enqueue_args``."""

    name: str = "foreign"


@actor(name="stream_remap_nul_target")
async def _nul_target(_payload: _TextPayload) -> None:
    pass


@actor(name="stream_remap_cap_healthy")
async def _cap_healthy(_payload: _TextPayload) -> None:
    pass


@actor(name="stream_remap_cap_capped", max_pending=1)
async def _cap_capped(_payload: _TextPayload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_client(backend: InMemoryBackend) -> JobsClient:
    return JobsClient(backend=backend, clock=FakeClock(start=_START))


def _stored_for(backend: InMemoryBackend, actor_name: str) -> int:
    return sum(1 for row in backend._jobs.values() if row.actor == actor_name)  # pyright: ignore[reportPrivateUsage]  # Why: test-only observation of the partial-commit surface - the durable committed prefix the caller must not blindly retry


def _text_items(count: int, *, bad_at: int | None = None) -> Iterable[EnqueueItem]:
    """A ``count``-item stream whose item ``bad_at`` (if given) carries a
    NUL byte in its payload - valid to pydantic, rejected by the shared
    jsonb NUL guard at the backend's batch preflight."""
    for i in range(count):
        text = "bad\x00value" if i == bad_at else f"value-{i}"
        yield EnqueueItem(actor_ref=_nul_target, payload=_TextPayload(text=text))


# ── One of each registry member, driven through the boundary ────────────


async def test_nul_payload_on_later_chunk_names_stream_global_index() -> None:
    """A NUL byte in item 4's payload of a 6-item stream (chunk_size=2)
    raises ``PayloadValidationError`` naming item 4 - the position in the
    CALLER's stream - while the two committed chunks (items 0-3) stay
    durably stored: the partial-commit surface a safe retry must respect.

    This is the proven defect: the backend's per-call NUL annotation
    carries the CHUNK-LOCAL index (0), which nothing remapped - the error
    confidently named a stream position that was never attempted, on a
    path where a blind retry duplicates the committed prefix.
    """
    backend = _make_backend()
    client = _make_client(backend)

    with pytest.raises(PayloadValidationError, match=r"for item 4") as exc_info:
        await client.enqueue_batch_streaming(_text_items(6, bad_at=4), chunk_size=2)

    # The remapped annotation preserves the guard's field attribution.
    assert "payload" in str(exc_info.value)
    # The committed prefix is observable: chunks 1-2 (items 0-3) are
    # durably stored; the failing chunk (items 4-5) admitted nothing.
    assert _stored_for(backend, _nul_target.name) == 4


async def test_pydantic_failure_on_later_chunk_names_stream_global_index() -> None:
    """A payload that fails pydantic validation at stream position 4
    raises ``PayloadValidationError`` naming item 4, with the committed
    prefix (items 0-3) durable - the pydantic arm of the same boundary."""
    backend = _make_backend()
    client = _make_client(backend)

    def _stream() -> Iterable[EnqueueItem]:
        for i in range(6):
            payload: BaseModel = _ForeignPayload() if i == 4 else _TextPayload(text=f"value-{i}")
            yield EnqueueItem(actor_ref=_nul_target, payload=payload)  # type: ignore[arg-type]  # Why: the wrong-model instance is the defect under test - build_enqueue_args must reject it

    with pytest.raises(PayloadValidationError, match=r"for item 4"):
        await client.enqueue_batch_streaming(_stream(), chunk_size=2)

    assert _stored_for(backend, _nul_target.name) == 4


async def test_cap_refusal_on_later_chunk_names_stream_global_indices() -> None:
    """A per-actor cap refusal on chunk 2 raises
    ``BatchMaxPendingExceededError`` with STREAM-GLOBAL refused indices
    and an ``admitted_count`` covering the committed prefix - the
    backpressure arm of the same boundary."""
    backend = _make_backend()
    client = _make_client(backend)
    # Fill the capped actor's single slot so its first stream item is refused.
    from taskq.client._args import build_enqueue_args

    await backend.enqueue(build_enqueue_args(_cap_capped, _TextPayload(text="seed"), max_pending=1))

    def _stream() -> Iterable[EnqueueItem]:
        yield EnqueueItem(actor_ref=_cap_healthy, payload=_TextPayload(text="0"))
        yield EnqueueItem(actor_ref=_cap_healthy, payload=_TextPayload(text="1"))
        yield EnqueueItem(actor_ref=_cap_capped, payload=_TextPayload(text="2"))
        yield EnqueueItem(actor_ref=_cap_healthy, payload=_TextPayload(text="3"))
        yield EnqueueItem(actor_ref=_cap_healthy, payload=_TextPayload(text="4"))

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await client.enqueue_batch_streaming(_stream(), chunk_size=2)

    err = exc_info.value
    # Stream-global: 2 is the capped item's position in the caller's
    # stream, not its chunk-local position (0) in the failing chunk.
    assert err.refused_indices == {_cap_capped.name: [2]}
    # The committed prefix folded in: chunk 1's two healthy items plus
    # the failing chunk's one healthy item.
    assert err.admitted_count == 3
    assert _stored_for(backend, _cap_healthy.name) == 3
    assert _stored_for(backend, _cap_capped.name) == 1


# ── The pattern itself: the registry is the boundary's one source ───────


def test_boundary_registry_is_the_single_exhaustive_source() -> None:
    """Every per-item error type the streaming boundary remaps is
    registered in ONE structure - ``_ITEM_ERROR_REMAPS`` - and the
    boundary's except clause consults exactly that registry (its
    exception tuple is derived from the registry's keys).

    The membership is pinned exhaustively: adding a remappable per-item
    error type without registering it (the defect class this file guards
    - it would silently inherit chunk-local indices) or removing an
    entry must change this assertion consciously.
    """
    from taskq.client._jobs import _ITEM_ERROR_REMAPS, _REMAPPABLE_ITEM_ERROR_TYPES

    assert set(_ITEM_ERROR_REMAPS) == {
        ValidationError,
        PayloadValidationError,
        BatchMaxPendingExceededError,
    }, "the registry must be the exhaustive list of remapped per-item error types"
    # The except clause's tuple is derived from the registry, never
    # hand-maintained alongside it.
    assert tuple(_ITEM_ERROR_REMAPS) == _REMAPPABLE_ITEM_ERROR_TYPES
    for remap in _ITEM_ERROR_REMAPS.values():
        assert callable(remap)
