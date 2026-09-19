"""The ATOMIC arm's per-item errors must name STREAM-GLOBAL indices.

``enqueue_batch_streaming``'s atomic arm (failure_policy/finalizer, no
caller connection) re-chunks the caller's stream INSIDE the backend:
``enqueue_batch_atomic`` consumes the lazy args generator chunk by
chunk, and every per-item error the bulk core raises from a chunk
carries a position in SOME chunk. The class defect this file pins
closed: those positions crossed as CHUNK-LOCAL indices - a NUL at
stream position 23 with chunk_size=10 surfaced as "item 3" - and no
client layer can repair that (the boundary that remaps the chunked
arm's errors sees only its own per-chunk calls; it cannot know the
backend's internal chunk base). The fix lives at the source: the
atomic chunk loop passes the consumed prefix as ``index_base`` to the
bulk core, so the per-item annotations name the position in the
CALLER's stream.

The in-memory mirror answers identically: it consumes the same chunks
and preflights each through the same shared jsonb guards at the same
base, where it previously surfaced a BARE ``ValueError(NUL_JSONB_ERROR)``
with no attribution at all - a divergence the in-memory equivalence
rule forbids (same observable answer on both backends).

The position is also pinned as a FIELD -
``PayloadValidationError.item_index`` - at every raise site that knows
an index (the bulk build loops, the mirror's chunk preflight, the
client's annotation helper and its remapping re-raises), so retries
and tooling read the coordinate instead of parsing the message. The
PG-side seam (``backend/_batch_sql.py``'s chunk loop → the bulk core's
``index_base``) is unreachable through the in-memory backend, so the
atomic arm's PG case is integration-marked and runs against real
Postgres; every other case here needs no Docker.
"""

from collections.abc import Iterable
from datetime import UTC, datetime

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.batch import EnqueueItem
from taskq.batch_policy import AbortBatchAfter
from taskq.client._jobs import JobsClient
from taskq.exceptions import PayloadValidationError
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)

# The scenario every atomic-arm case shares: 25 items, chunk_size=10,
# the NUL at stream position 23 - chunk 3 (items 20-24), chunk-local
# position 3. Pre-fix, both backends named an index in the WRONG
# coordinate space ("item 3" on PG, a bare ValueError in memory).
_COUNT = 25
_CHUNK = 10
_BAD_AT = 23


class _TextPayload(BaseModel):
    text: str = "ok"


class _ForeignPayload(BaseModel):
    """A payload of the wrong model: passes construction, fails the
    actor's ``payload_type`` validation inside the args build."""

    name: str = "foreign"


@actor(name="atomic_remap_nul_target")
async def _nul_target(_payload: _TextPayload) -> None:
    pass


@actor(name="atomic_remap_pg_nul_target")
async def _pg_nul_target(_payload: _TextPayload) -> None:
    pass


@actor(name="atomic_remap_pg_fast_target")
async def _pg_fast_target(_payload: _TextPayload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_client(backend: InMemoryBackend) -> JobsClient:
    return JobsClient(backend=backend, clock=FakeClock(start=_START))


def _stored_for(backend: InMemoryBackend, actor_name: str) -> int:
    return sum(1 for row in backend._jobs.values() if row.actor == actor_name)  # pyright: ignore[reportPrivateUsage]  # Why: test-only observation of the atomic arm's rollback - the nothing-stored contract the caller's retry decision rests on


def _text_items(count: int, *, bad_at: int | None = None) -> Iterable[EnqueueItem]:
    """A ``count``-item stream whose item ``bad_at`` (if given) carries a
    NUL byte in its payload - valid to pydantic, rejected by the shared
    jsonb NUL guard at the backend's per-chunk build/preflight."""
    for i in range(count):
        text = "bad\x00value" if i == bad_at else f"value-{i}"
        yield EnqueueItem(actor_ref=_nul_target, payload=_TextPayload(text=text))


# ── The atomic arm: stream-global indices on both backends ───────────


async def test_atomic_nul_at_later_stream_position_names_stream_global_index() -> None:
    """A NUL at stream position 23 of a 25-item stream (chunk_size=10)
    raises ``PayloadValidationError`` naming item 23 - the position in
    the CALLER's stream, not chunk-local position 3 - with the position
    carried in ``item_index``, and the atomic rollback leaves nothing
    stored.

    This is the in-memory half of the parity pair: the mirror consumes
    the same chunks the PG arm does and preflights each through the
    same shared guards at the same base, where it previously surfaced a
    BARE ``ValueError`` with no item attribution at all.
    """
    backend = _make_backend()
    client = _make_client(backend)

    with pytest.raises(PayloadValidationError, match=r"for item 23") as exc_info:
        await client.enqueue_batch_streaming(
            _text_items(_COUNT, bad_at=_BAD_AT),
            failure_policy=AbortBatchAfter(3),
            chunk_size=_CHUNK,
        )

    err = exc_info.value
    assert err.item_index == _BAD_AT
    # The annotation preserves the guard's field attribution.
    assert "payload" in str(err)
    # The atomic contract: one transaction, rolled back whole - nothing
    # stored, so the caller retries the entire stream safely.
    assert _stored_for(backend, _nul_target.name) == 0


async def test_atomic_pydantic_failure_names_stream_global_index() -> None:
    """A payload that fails pydantic validation at stream position 23 of
    the atomic arm raises ``PayloadValidationError`` naming item 23 with
    ``item_index == 23`` - the client's lazy generator enumerates the
    WHOLE stream, so its annotation site is stream-global by
    construction - and the rollback leaves nothing stored."""
    backend = _make_backend()
    client = _make_client(backend)

    def _stream() -> Iterable[EnqueueItem]:
        for i in range(_COUNT):
            payload: BaseModel = (
                _ForeignPayload() if i == _BAD_AT else _TextPayload(text=f"value-{i}")
            )
            yield EnqueueItem(actor_ref=_nul_target, payload=payload)  # type: ignore[arg-type]  # Why: the wrong-model instance is the defect under test - build_enqueue_args must reject it

    with pytest.raises(PayloadValidationError, match=r"for item 23") as exc_info:
        await client.enqueue_batch_streaming(
            _stream(), failure_policy=AbortBatchAfter(3), chunk_size=_CHUNK
        )

    assert exc_info.value.item_index == _BAD_AT
    assert _stored_for(backend, _nul_target.name) == 0


@pytest.mark.integration
async def test_atomic_nul_at_later_stream_position_names_stream_global_index_on_pg(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """The same pin against real Postgres - the seam the in-memory
    backend cannot reach: PG's ``enqueue_batch_atomic`` re-chunks the
    stream inside the backend and each chunk crosses the bulk build
    loop, whose per-item NUL guard now annotates at ``index_base`` +
    the chunk position. Pre-fix, this arm named CHUNK-LOCAL position 3
    while confidently pointing the caller's retry at a stream position
    that was never attempted.

    Parity with the in-memory case above is the point: same scenario,
    same exception type, same stream-global index, same nothing-stored
    answer.
    """
    from taskq.client._jobs import JobsClient as _JobsClient

    from .test_rt_cron_harness import count_jobs, cron_settings, pool_backend

    schema = module_pg_schema.schema_name
    backend = pool_backend(cron_settings(schema), module_pg_pool)
    client = _JobsClient(backend)

    def _stream() -> Iterable[EnqueueItem]:
        for i in range(_COUNT):
            text = "bad\x00value" if i == _BAD_AT else f"value-{i}"
            yield EnqueueItem(actor_ref=_pg_nul_target, payload=_TextPayload(text=text))

    with pytest.raises(PayloadValidationError, match=r"for item 23") as exc_info:
        await client.enqueue_batch_streaming(
            _stream(), failure_policy=AbortBatchAfter(3), chunk_size=_CHUNK
        )

    err = exc_info.value
    assert err.item_index == _BAD_AT
    assert "payload" in str(err)
    # The single transaction rolled back: chunks 1-2 (items 0-19)
    # inserted inside it are discarded with the failing chunk 3.
    assert await count_jobs(clean_pg_conn, schema, _pg_nul_target.name) == 0


# ── The field: every raise site that knows an index populates it ─────


async def test_chunked_arm_nul_remap_populates_item_index() -> None:
    """The CHUNKED (non-atomic) arm's registry remap re-raises the
    located NUL item at its stream-global index through the shared
    ``_nul_item_payload_error`` helper - the field must ride along
    (``item_index == 4`` for the precedent scenario: NUL at position 4
    of a 6-item stream, chunk_size=2), with the committed prefix
    durable exactly as the precedent file pins."""
    backend = _make_backend()
    client = _make_client(backend)

    with pytest.raises(PayloadValidationError, match=r"for item 4") as exc_info:
        await client.enqueue_batch_streaming(_text_items(6, bad_at=4), chunk_size=2)

    assert exc_info.value.item_index == 4
    assert _stored_for(backend, _nul_target.name) == 4


async def test_chunked_arm_pydantic_remap_populates_item_index() -> None:
    """The chunked arm's pydantic remap re-annotates through the
    client's ``_item_payload_error`` at ``chunk_offset + position`` -
    the field must ride along there too."""
    backend = _make_backend()
    client = _make_client(backend)

    def _stream() -> Iterable[EnqueueItem]:
        for i in range(6):
            payload: BaseModel = _ForeignPayload() if i == 4 else _TextPayload(text=f"value-{i}")
            yield EnqueueItem(actor_ref=_nul_target, payload=payload)  # type: ignore[arg-type]  # Why: the wrong-model instance is the defect under test - build_batch_args must reject it

    with pytest.raises(PayloadValidationError, match=r"for item 4") as exc_info:
        await client.enqueue_batch_streaming(_stream(), chunk_size=2)

    assert exc_info.value.item_index == 4
    assert _stored_for(backend, _nul_target.name) == 4


async def test_item_index_defaults_to_none_without_an_item_coordinate() -> None:
    """Backward compatibility of the field's default: raise sites with
    no item coordinate (single-item enqueue validation) leave
    ``item_index`` as ``None`` - the field is additive, not a new
    requirement on every constructor call."""
    from taskq._validation import validate_actor_payload

    with pytest.raises(PayloadValidationError) as exc_info:
        validate_actor_payload(_TextPayload, _ForeignPayload(), actor=_nul_target.name)

    assert exc_info.value.item_index is None


@pytest.mark.integration
async def test_fast_arm_nul_names_item_index_on_pg(clean_jobs_app: JobsApp) -> None:
    """The COPY arm's build loop populates the field too - driven
    straight at the backend (its caller always passes the whole list,
    so the index is the list position; the ``index_base`` plumbing is
    the same shared-guard call the atomic arm shifts). A NUL at list
    position 2 of 3 names item 2 in message and field alike."""
    from taskq._ids import new_job_id
    from taskq.backend._protocol import EnqueueArgs

    backend = clean_jobs_app.backend
    args_list = [
        EnqueueArgs(
            id=new_job_id(),
            actor=_pg_fast_target.name,
            queue="default",
            payload={"text": "bad\x00value"} if i == 2 else {"text": f"value-{i}"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        )
        for i in range(3)
    ]

    with pytest.raises(PayloadValidationError, match=r"for item 2") as exc_info:
        await backend.enqueue_batch_fast(args_list)

    assert exc_info.value.item_index == 2
