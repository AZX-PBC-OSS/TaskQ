"""Actor registry the OTLP round-trip worker consumes.

Imported by path (``tests.otel_validation.otel_roundtrip_actors:_REGISTRY``)
from the worker subprocess the round-trip test spawns, and imported directly
by the test process to enqueue the same payloads through the public client.
Two actors, one outcome each:

- ``otel_roundtrip_ok`` succeeds, producing the CONSUMER span and the
  consumed-messages metric for the success path.
- ``otel_roundtrip_fail`` always raises, with a URI-shaped credential in the
  message. The failure handlers record the attempt-failure counter for it,
  and the exception text that reaches the attempt span must arrive in the
  collector's output with the credential masked.
"""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel

from taskq.actor import ActorRef, actor


class Note(BaseModel):
    text: str


@actor(name="otel_roundtrip_ok", queue="default")
async def ok_job(payload: Note) -> None:
    del payload


@actor(name="otel_roundtrip_fail", queue="default", non_retryable_exceptions=(RuntimeError,))
async def fail_job(payload: Note) -> None:
    del payload
    raise RuntimeError(
        "payment database unreachable: postgresql://job_writer:sup3r-sekret@db.internal:5432/jobs"
    )


_REGISTRY: Mapping[str, ActorRef[Any, Any]] = MappingProxyType(
    {
        "otel_roundtrip_ok": ok_job,
        "otel_roundtrip_fail": fail_job,
    }
)
