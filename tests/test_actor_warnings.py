"""Tests for first-enqueue warning when unique_for is set but identity_key is absent.

Covers:
  - first enqueue with unique_for and no identity_key still succeeds
  - Warned-once: second enqueue with same actor still succeeds (no crash)
  - unique_for=None: enqueue succeeds normally
  - unique_for with identity_key present: enqueue succeeds normally

The log-format details are intentionally not asserted — those are
implementation details that change independently of behaviour.
"""

from datetime import UTC, datetime, timedelta

from pydantic import BaseModel

from taskq.actor import actor
from taskq.backend._protocol import IdentityKey
from taskq.client._jobs import JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_EPOCH = datetime(2025, 1, 1, tzinfo=UTC)


class _WarnPayload(BaseModel):
    value: int = 1


@actor(name="warn_actor_a", unique_for=timedelta(minutes=15))
async def _warn_actor_a(payload: _WarnPayload) -> None:
    pass


@actor(name="warn_actor_b", unique_for=timedelta(minutes=30))
async def _warn_actor_b(payload: _WarnPayload) -> None:
    pass


@actor(name="nowarn_no_unique_for")
async def _nowarn_no_unique_for(payload: _WarnPayload) -> None:
    pass


@actor(name="nowarn_with_identity", unique_for=timedelta(minutes=15))
async def _nowarn_with_identity(payload: _WarnPayload) -> None:
    pass


def _make_client() -> JobsClient:
    backend = InMemoryBackend(clock=FakeClock(_EPOCH))
    return JobsClient(backend)


async def test_first_enqueue_unique_for_no_identity_succeeds() -> None:
    client = _make_client()
    payload = _WarnPayload(value=42)

    handle = await client.enqueue(_warn_actor_a, payload)
    assert handle.job_id is not None


async def test_second_enqueue_same_actor_succeeds() -> None:
    client = _make_client()
    payload = _WarnPayload(value=42)

    h1 = await client.enqueue(_warn_actor_a, payload)
    h2 = await client.enqueue(_warn_actor_a, payload)
    assert h1.job_id != h2.job_id


async def test_different_actors_each_succeed() -> None:
    client = _make_client()
    payload = _WarnPayload(value=42)

    h_a = await client.enqueue(_warn_actor_a, payload)
    h_b = await client.enqueue(_warn_actor_b, payload)
    assert h_a.job_id != h_b.job_id


async def test_unique_for_none_succeeds() -> None:
    client = _make_client()
    payload = _WarnPayload(value=42)

    handle = await client.enqueue(_nowarn_no_unique_for, payload)
    assert handle.job_id is not None


async def test_unique_for_with_identity_key_succeeds() -> None:
    client = _make_client()
    payload = _WarnPayload(value=42)

    handle = await client.enqueue(
        _nowarn_with_identity,
        payload,
        identity_key=IdentityKey("account:42"),
    )
    assert handle.job_id is not None
